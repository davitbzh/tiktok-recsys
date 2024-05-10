# Setup the feature groups for the Flink pipelines
import pandas as pd
import hopsworks
from hsfs.feature import Feature
from datetime import datetime, timedelta, timezone

project = hopsworks.login()
fs = project.get_feature_store()

features = [
    Feature(name="ticker", type="string"),
    Feature(name="open", type="float"),
    Feature(name="high", type="float"),
    Feature(name="low", type="float"),
    Feature(name="close", type="float"),
    Feature(name="volume", type="float"),
    Feature(name="vwap", type="float"),
    Feature(name="t", type="timestamp"),
    Feature(name="transactions", type="bigint"),
    Feature(name="otc", type="string"),
]

ticker_fg = fs.get_or_create_feature_group(
    name="ticker",
    description="ticker data.",
    version=1,
    primary_key=["ticker"],
#    partition_key=["interaction_month"],
    online_enabled=True,
    event_time="t"
)

ticker_fg.save(features)
