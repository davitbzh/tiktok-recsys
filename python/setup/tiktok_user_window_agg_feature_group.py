import hopsworks

from hsfs.feature import Feature
from datetime import datetime, timedelta, timezone

project = hopsworks.login()
fs = project.get_feature_store()

features = [
    Feature(name="user_id", type="string"),
    Feature(name="video_category", type="string"),

    Feature(name="like_count", type="bigint"),
    Feature(name="dislike_count", type="bigint"),
    Feature(name="view_count", type="bigint"),
    Feature(name="comment_count", type="bigint"),
    Feature(name="share_count", type="bigint"),
    Feature(name="skip_count", type="bigint"),
    Feature(name="total_watch_time", type="bigint"),

    Feature(name="interaction_day", type="string"),
    Feature(name="window_end_time", type="timestamp"),
]

fg = fs.create_feature_group(
    "user_window_agg_1h",
    version=1,
    statistics_config=False,
    primary_key=["user_id"],
    partition_key=["interaction_day"],
    event_time="window_end_time",
    online_enabled=True,
    stream=True,
)

fg.save(features)

fg.materialization_job.schedule(cron_expression="0 */15 * ? * *",
                                start_time=datetime.now(tz=timezone.utc))
