import os
import time
import requests
import argparse

import hopsworks
from hsfs import engine

import pem

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")


# https://www.feldera.com/docs/tutorials/rest_api/
def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080)")
    parser.add_argument("--prepare-args", required=False, help="number of SecOps pipelines to simulate")
    parser.add_argument("--kafka-url-for-connector", required=False, default="redpanda:9092",
                        help="Kafka URL from pipeline")
    parser.add_argument("--registry-url-for-connector", required=False, default="http://redpanda:8081",
                        help="Schema registry URL from pipeline")

    parser.add_argument("--hopsworks-host", required=True, help="Hopsworks URL (e.g., https://localhost:8181 )")
    parser.add_argument("--hopsworks-project-name", required=True, help="Project Name in Hopsworks cluster")
    parser.add_argument("--hopsworks-api-key", required=True, help="Api key for Hopsworks cluster")

    args = parser.parse_args()
    prepare_feldera(args.api_url,
                    args.kafka_url_for_connector,
                    args.hopsworks_host,
                    args.hopsworks_project_name,
                    args.hopsworks_api_key)


def prepare_feldera(api_url, pipeline_to_redpanda_server, hopsworks_host, hopsworks_project_name, hopsworks_api_key):
    # Create program
    program_name = "hopsworks-demo-sec-ops-program"
    program_sql = open(PROJECT_SQL).read()
    response = requests.put(f"{api_url}/v0/programs/{program_name}", json={
        "description": "",
        "code": program_sql
    })
    response.raise_for_status()
    program_version = response.json()["version"]

    # Compile program
    print(f"Compiling program {program_name} (version: {program_version})...")
    requests.post(f"{api_url}/v0/programs/{program_name}/compile", json={"version": program_version}).raise_for_status()
    while True:
        status = requests.get(f"{api_url}/v0/programs/{program_name}").json()["status"]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(5)

    # Connectors
    connectors = []
    for (connector_name, stream, topic_topics, is_input) in [
        ("secops_pipeline", 'PIPELINE', ["secops_pipeline"], True),
        ("secops_pipeline_sources", 'PIPELINE_SOURCES', ["secops_pipeline_sources"], True),
        ("secops_artifact", 'ARTIFACT', ["secops_artifact"], True),
        ("secops_vulnerability", 'VULNERABILITY', ["secops_vulnerability"], True),
        ("secops_cluster", 'K8SCLUSTER', ["secops_cluster"], True),
        ("secops_k8sobject", 'K8SOBJECT', ["secops_k8sobject"], True),
        #        ("secops_vulnerability_stats", 'K8SCLUSTER_VULNERABILITY_STATS', "secops_vulnerability_stats", False),
    ]:
        requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
            "description": "",
            "config": {
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete"
                    }
                },
                "transport": {
                    "name": "kafka_" + ("input" if is_input else "output"),
                    "config": {
                        "bootstrap.servers": pipeline_to_redpanda_server,
                        "topic": topic_topics
                    } if not is_input else (
                        {
                            "bootstrap.servers": pipeline_to_redpanda_server,
                            "topics": topic_topics,
                            "auto.offset.reset": "earliest",
                            "group.id": "secops_pipeline_sources",
                            "enable.auto.commit": "true",
                            "enable.auto.offset.store": "true",
                        }
                        if stream == "PIPELINE_SOURCES" else
                        {
                            "bootstrap.servers": pipeline_to_redpanda_server,
                            "topics": topic_topics,
                            "auto.offset.reset": "earliest"
                        }
                    )
                }
            }
        })
        connectors.append({
            "connector_name": connector_name,
            "is_input": is_input,
            "name": connector_name,
            "relation_name": stream
        })

    # Hopsworks part
    project = hopsworks.login(
        host=hopsworks_host,
        project=hopsworks_project_name,
        api_key_value=hopsworks_api_key)

    fs = project.get_feature_store()

    # it is assumed that feature group was already created
    feature_group = fs.get_feature_group("k8_vulnerability", 1)

    # get feature group schema
    schema = feature_group.avro_schema

    # get kafka connection config
    hopsworks_kafka_config = engine.get_instance()._get_kafka_config(feature_group.feature_store_id, {})
    hopsworks_kafka_config["topic"] = feature_group._online_topic_name

    """
    ca = hopsworks_kafka_config.pop('ssl.ca.location')
    certificate = hopsworks_kafka_config.pop('ssl.certificate.location')
    key = hopsworks_kafka_config.pop('ssl.key.location')
    hopsworks_kafka_config["ssl.ca.pem"] = str(pem.parse_file(ca)[0])
    hopsworks_kafka_config["ssl.certificate.pem"] = str(pem.parse_file(certificate)[0])
    hopsworks_kafka_config["ssl.key.pem"] = str(pem.parse_file(key)[0])
    """

    headers = [
        {"key": "projectId", "value": str(feature_group.feature_store.project_id)},
        {"key": "featureGroupId", "value": str(feature_group._id)},
        {"key": "subjectId", "value": str(feature_group.subject["id"])},
    ]

    hopsworks_kafka_config["headers"] = headers
    requests.put(f"{api_url}/v0/connectors/secops_vulnerability_stats_fg", json={
        "description": "",
        "config": {
            "format": {
                "name": "avro",
                "config": {
                    "schema": schema
                }
            },

            "transport": {
                "name": "kafka_output",
                "config": hopsworks_kafka_config,
            }
        }
    })

    connectors.append({
        "connector_name": "secops_vulnerability_stats_fg",
        "is_input": False,
        "name": "secops_vulnerability_stats_fg",
        "relation_name": "k8scluster_vulnerability_stats"
    })

    # Create pipeline
    pipeline_name = "hopsworks-demo-sec-ops-pipeline"
    requests.put(f"{api_url}/v0/pipelines/{pipeline_name}", json={
        "description": "",
        "config": {"workers": 8},
        "program_name": program_name,
        "connectors": connectors,
    }).raise_for_status()


if __name__ == "__main__":
    main()
