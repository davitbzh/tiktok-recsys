# Setup the feature groups for the Flink pipelines
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
    partition_key=["interaction_day"],
    online_enabled=True,
    event_time="interaction_date"

)

interactions_fg.save(features)

interactions_fg.materialization_job.schedule(cron_expression="0 */15 * ? * *",
                                             start_time=datetime.now(tz=timezone.utc))
