import hopsworks

from hsfs.feature import Feature
from datetime import datetime, timedelta, timezone

project = hopsworks.login()
fs = project.get_feature_store()

features = [
    Feature(name="video_id", type="string"),
    Feature(name="category_id", type="bigint"),

    Feature(name="like_count", type="bigint"),
    Feature(name="dislike_count", type="bigint"),
    Feature(name="view_count", type="bigint"),
    Feature(name="comment_count", type="bigint"),
    Feature(name="share_count", type="bigint"),
    Feature(name="skip_count", type="bigint"),
    Feature(name="total_watch_time", type="bigint"),

    Feature(name="interaction_month", type="string"),
    Feature(name="window_end_time", type="timestamp"),
]

video_window_agg_1h_fg = fs.create_feature_group(
    "video_window_agg_1h",
    version=1,
    statistics_config=False,
    primary_key=["video_id"],
    partition_key=["interaction_month"],
    event_time="window_end_time",
    online_enabled=True,
    stream=True,
)

video_window_agg_1h_fg.save(features)

video_window_agg_1h_fg.materialization_job.schedule(cron_expression="0 */15 * ? * *",
                                                    start_time=datetime.now(tz=timezone.utc))

feature_descriptions = [
    {"name": "video_id", "description": "Identifier for the video."},
    {"name": "category_id", "description": "Id of the video category."},
    {"name": "window_end_time", "description": "End of the specified time window where interaction were aggregated."},
    {"name": "interaction_month",
     "description": "Month of the end of the specified time window where interaction were aggregated. Derived from window_end_time"},
    {"name": "like_count", "description": "Number of likes video got over a specified time window."},
    {"name": "dislike_count", "description": "Number of dislikes video got over a specified time window."},
    {"name": "view_count", "description": "Number of views video got over a specified time window."},
    {"name": "comment_count", "description": "Number of comments video got over a specified time window."},
    {"name": "share_count", "description": "Number of likes over got over a specified time window."},
    {"name": "skip_count", "description": "Number of times video was skiped over a specified time window."},
    {"name": "total_watch_time",
     "description": "Total time in seconds video was watched over a specified time window."},
]

for desc in feature_descriptions:
    video_window_agg_1h_fg.update_feature_description(desc["name"], desc["description"])

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
    "pii": False,
    "data_sensitivity": "Low",
    "business_unit": "Credit Cards"
}

# Attach the tag
video_window_agg_1h_fg.add_tag("data_privacy_ownership", tag)

video_window_agg_1h_fg.statistics_config = {
    "enabled": True,
    "histograms": True,
    "correlations": True,
}

video_window_agg_1h_fg.update_statistics_config()
video_window_agg_1h_fg.compute_statistics()