"""Prepares Elasticsearch and Kibana for the vehicle telemetry data.

1. Installs the index template that schema/generate.py generated from the
   .proto files, so new daily indices get the right field types (e.g.
   `position` as a geo_point, so it can be shown on a map).
2. Adds new fields to the mapping of existing indices, so a schema change
   takes effect today instead of tomorrow. (Changing the type of an existing
   field isn't possible in Elasticsearch; that only applies to new indices.)
3. Creates the Kibana data view.
"""

import json
import os
from pathlib import Path

import requests

ES = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
KIBANA = os.getenv("KIBANA_URL", "http://kibana:5601")
TEMPLATE_NAME = "vehicle-telemetry"
TEMPLATE_FILE = Path(__file__).resolve().parent.parent.parent / "schema/generated/elasticsearch/index_template.json"


def main() -> None:
    template = json.loads(TEMPLATE_FILE.read_text())

    print(f"Installing index template '{TEMPLATE_NAME}' (generated from {template['_meta']['generated_from']})")
    requests.put(f"{ES}/_index_template/{TEMPLATE_NAME}", json=template, timeout=30).raise_for_status()

    pattern = ",".join(template["index_patterns"])
    response = requests.put(
        f"{ES}/{pattern}/_mapping",
        params={"allow_no_indices": "true"},
        json=template["template"]["mappings"],
        timeout=30,
    )
    if response.ok:
        print("Updated the mapping of existing indices")
    else:
        # Typically: a field changed type. New indices get the new type.
        print(f"Could not update the mapping of existing indices: {response.text}")

    print("Creating Kibana data view 'vehicle-telemetry-*'")
    requests.post(
        f"{KIBANA}/api/data_views/data_view",
        headers={"kbn-xsrf": "true"},
        json={
            "override": True,
            "data_view": {
                "id": "vehicle-telemetry",
                "name": "Vehicle telemetry",
                "title": pattern,
                "timeFieldName": "@timestamp",
            },
        },
        timeout=30,
    ).raise_for_status()
    print("Done.")


if __name__ == "__main__":
    main()
