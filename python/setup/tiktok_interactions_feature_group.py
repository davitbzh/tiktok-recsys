# Setup the feature groups for the Flink pipelines
import pandas as pd
import hopsworks
from hsfs.feature import Feature
from datetime import datetime, timedelta, timezone

project = hopsworks.login()
fs = project.get_feature_store()

features = [
    Feature(name="interaction_month", type="string"),
    Feature(name="interaction_id", type="string"),
    Feature(name="user_id", type="string"),
    Feature(name="video_id", type="string"),
    Feature(name="category_id", type="bigint"),
    Feature(name="interaction_type", type="string"),
    Feature(name="watch_time", type="bigint"),
    Feature(name="interaction_date", type="timestamp"),
]

interactions_fg = fs.get_or_create_feature_group(
    name="interactions",
    description="Interactions data.",
    version=1,
    primary_key=["interaction_id", "user_id", "video_id"],
    partition_key=["interaction_month"],
    online_enabled=True,
    event_time="interaction_date"

)

interactions_fg.save(features)

feature_descriptions = [
    {"name": "interaction_id", "description": "Unique id for the interaction"},
    {"name": "user_id", "description": "Unique identifier for each user."},
    {"name": "video_id", "description": "Identifier for the video."},
    {"name": "category_id", "description": "Id of the video category."},
    {"name": "interaction_type", "description": "Type of interaction"},
    {"name": "watch_time", "description": "Time in seconds how long user watched the video."},
    {"name": "interaction_date", "description": "Date of inteaction."},
    {"name": "interaction_month", "description": "Month of interaction, derived from interaction_date."}
]

for desc in feature_descriptions:
    interactions_fg.update_feature_description(desc["name"], desc["description"])

# Define tag values
tag = {
    "org_level": "Managing Director",
    "project": "MDLC",
    "firewall": "Inside",
    "security_review": True,
    "reliability": "Extreme",
    "expected_reusability": "Extreme",
    "expected_uplift": "Extreme",
    "draft_publish": "Publish",
    "environment": "Production",
    "business_function": "Sales",
    "division": "CCB",
    "data_source": "Kafka",
    "pii": True,
    "data_sensitivity": "High",
    "business_unit": "Credit Cards"
}

# Attach the tag
interactions_fg.add_tag("data_privacy_ownership", tag)

interactions_fg.materialization_job.schedule(cron_expression="0 */15 * ? * *",
                                             start_time=datetime.now(tz=timezone.utc))
