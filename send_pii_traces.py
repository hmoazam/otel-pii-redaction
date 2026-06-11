"""
Send PII test data as OTel spans to a Databricks workspace.
Each JSONL line becomes a trace with a root span containing the PII in attributes.

Usage:
  python send_pii_traces.py <WORKSPACE_HOST> <TABLE_NAME> [DATA_FILE]

Example:
  python send_pii_traces.py https://my-workspace.cloud.databricks.com my_catalog.my_schema.my_prefix_otel_spans
  python send_pii_traces.py https://my-workspace.cloud.databricks.com my_catalog.my_schema.my_prefix_otel_spans custom_data.jsonl
"""

import json
import subprocess
import sys
import time

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    workspace_host = sys.argv[1]
    table_name = sys.argv[2]
    data_file = sys.argv[3] if len(sys.argv) > 3 else "pii_test_data.jsonl"

    # Get token from Databricks CLI
    token_json = subprocess.check_output(
        ["databricks", "auth", "token", "--host", workspace_host],
        text=True,
    )
    token = json.loads(token_json)["access_token"]

    # Set up OTel exporter
    exporter = OTLPSpanExporter(
        endpoint=f"{workspace_host}/api/2.0/otel/v1/traces",
        headers={
            "Authorization": f"Bearer {token}",
            "X-Databricks-UC-Table-Name": table_name,
        },
    )

    resource = Resource.create({
        "service.name": "pii-test-data-generator",
        "deployment": "otel-pii-redaction-demo",
    })

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("pii-test-generator", "1.0.0")

    # Read and send each line as a separate trace
    with open(data_file, "r") as f:
        lines = f.readlines()

    print(f"Sending {len(lines)} PII test traces to {workspace_host}...")
    print(f"Target table: {table_name}")
    print()

    for i, line in enumerate(lines, 1):
        data = json.loads(line)
        prompt = data["prompt"]
        response = data["response"]

        with tracer.start_as_current_span(
            name="pii-test-interaction",
            attributes={
                "mlflow.spanInputs": json.dumps({"prompt": prompt}),
                "mlflow.spanOutputs": json.dumps({"response": response}),
                "test.line_number": i,
                "test.purpose": "pii-redaction-validation",
            },
        ):
            time.sleep(0.05)

        if i % 10 == 0:
            print(f"  Sent {i}/{len(lines)} traces")

    # Flush remaining spans
    provider.shutdown()
    print(f"\nDone! Sent {len(lines)} traces.")
    print("Wait ~1 minute for ingestion, then run the pipeline to redact them.")


if __name__ == "__main__":
    main()
