#!/bin/sh
# Prepares Elasticsearch and Kibana for the vehicle telemetry data.
set -e

ES=http://elasticsearch:9200
KIBANA=http://kibana:5601

echo "Installing index template 'vehicle-telemetry'..."
# Without this template Elasticsearch would guess the field types, and
# `position` would become two plain numbers instead of a geo_point.
curl -fsS -X PUT "$ES/_index_template/vehicle-telemetry" \
  -H 'Content-Type: application/json' \
  -d '{
    "index_patterns": ["vehicle-telemetry-*"],
    "template": {
      "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
      "mappings": {
        "properties": {
          "@timestamp":         { "type": "date" },
          "timestamp":          { "type": "date" },
          "message_id":         { "type": "keyword" },
          "vehicle_id":         { "type": "keyword" },
          "vin":                { "type": "keyword" },
          "vehicle_type":       { "type": "keyword" },
          "position":           { "type": "geo_point" },
          "heading_deg":        { "type": "float" },
          "speed_kmh":          { "type": "float" },
          "rpm":                { "type": "integer" },
          "gear":               { "type": "integer" },
          "engine_temp_c":      { "type": "float" },
          "fuel_level_pct":     { "type": "float" },
          "battery_voltage":    { "type": "float" },
          "odometer_km":        { "type": "double" },
          "tire_pressure_kpa": {
            "properties": {
              "front_left":  { "type": "float" },
              "front_right": { "type": "float" },
              "rear_left":   { "type": "float" },
              "rear_right":  { "type": "float" }
            }
          },
          "check_engine_light": { "type": "boolean" },
          "dtc_codes":          { "type": "keyword" },
          "events":             { "type": "keyword" },
          "kafka": {
            "properties": {
              "topic":     { "type": "keyword" },
              "partition": { "type": "integer" },
              "offset":    { "type": "long" }
            }
          }
        }
      }
    }
  }'
echo

echo "Creating Kibana data view 'vehicle-telemetry-*'..."
curl -fsS -X POST "$KIBANA/api/data_views/data_view" \
  -H 'Content-Type: application/json' \
  -H 'kbn-xsrf: true' \
  -d '{
    "override": true,
    "data_view": {
      "id": "vehicle-telemetry",
      "name": "Vehicle telemetry",
      "title": "vehicle-telemetry-*",
      "timeFieldName": "@timestamp"
    }
  }' > /dev/null
echo "Done."
