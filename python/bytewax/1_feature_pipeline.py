import statistics
import json
from datetime import datetime, timedelta, timezone

import bytewax.operators.window as win
from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.kafka import KafkaSourceMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.operators.window import EventClockConfig, TumblingWindow, SlidingWindow

import hopsworks
from hsfs_bytewax_util import get_kafka_config, serialize_with_key, sink_kafka
from interactions import generate_live_interactions


# This is the accumulator function, and outputs a list of 2-tuples,
# containing the event's "value" and it's "time" (used later to print info)
def accumulate(acc, event):
    acc.append(event)
    return acc


# This function instructs the event clock on how to retrieve the
# event's datetime from the input.
# Note that the datetime MUST be UTC. If the datetime is using a different
# representation, we would have to convert it here.
def get_event_time(event):
    # return datetime.strptime(event["interaction_date"], "%Y-%m-%d %H:%M:%S").replace(
    #    tzinfo=timezone.utc
    # )
    return (event["interaction_date"].replace(
        tzinfo=timezone.utc
    ))


def user_interaction_event(event):
    pass


def video_interaction_event(event):
    key, (metadata, data) = event
    likes = [x['interaction_type'] for x in data if x['interaction_type'] == "like"]
    dislikes = [x['interaction_type'] for x in data if x['interaction_type'] == "dislike"]
    views = [x['interaction_type'] for x in data if x['interaction_type'] == "view"]
    comments = [x['interaction_type'] for x in data if x['interaction_type'] == "comment"]
    shares = [x['interaction_type'] for x in data if x['interaction_type'] == "share"]
    skips = [x['interaction_type'] for x in data if x['interaction_type'] == "skip"]
    watch_times = [x['interaction_type'] for x in data if x['interaction_type'] == "watch_time"]

    return key, {
        "video_id": key,

        'window_end_time': metadata.close_time.replace(tzinfo=timezone.utc),
        'interaction_day': metadata.close_time.replace(tzinfo=timezone.utc).strftime('%Y-%m-%d'),

        "like_count": len(likes),
        "dislike_count": len(dislikes),
        "view_count": len(views),
        "comment_count": len(comments),
        "share_count": len(shares),
        "skip_count": len(skips),
        "total_watch_time": sum(watch_times),
    }


def get_flow(hopsworks_host, hopsworks_project, hopsworks_api_key):
    # connect to hopsworks
    project = hopsworks.login(
        host=hopsworks_host,
        project=hopsworks_project,
        api_key_value=hopsworks_api_key
    )
    fs = project.get_feature_store()

    # get kafka connection config
    kafka_config = get_kafka_config(feature_store_id=fs.id)
    kafka_config["auto.offset.reset"] = "earliest"

    flow = Dataflow("windowing")
    align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)

    # This will pass simulated transactions directly to the streaming pipeline
    inp = generate_live_interactions(1000000)
    parsed_stream = op.input("input", flow, TestingSource(inp))

    #######################################################
    # Group the readings by account, so that we only
    # aggregate readings of the same type.
    user_keyed_stream = op.key_on("key_on_user", parsed_stream, lambda e: e["user_id"])

    ###########################################################
    # get feature group and its topic configuration
    feature_group = fs.get_feature_group("interactions", 1)

    # sync to feature group topic
    fg_serialized_stream = op.map(
        "interactions_fg-serialize_with_key",
        user_keyed_stream,
        lambda x: serialize_with_key(x, feature_group),
    )

    processed = op.map(
        "map", fg_serialized_stream, lambda x: sink_kafka(x[0], x[1], feature_group)
    )

    ###########################################################
    # Configure the `fold_window` operator to use the event time.
    clock = EventClockConfig(
        get_event_time, wait_for_system_duration=timedelta(seconds=10)
    )

    windower = SlidingWindow(
        length=timedelta(minutes=5),
        offset=timedelta(minutes=1),
        align_to=align_to,
    )

    # user_windowed_stream = win.fold_window(
    #    "user_windowed_stream", user_keyed_stream, clock, windower, dict, accumulate
    # )
    # op.inspect("inspect-user-windowed-stream", user_windowed_stream)
    # windowed_stream = op.map("user_interaction_event", user_windowed_stream, user_interaction_event)

    video_window_agg_feature_group = fs.get_feature_group("video_window_agg_1h", 1)
    video_keyed_stream = op.key_on("key_on_video", parsed_stream, lambda e: e["video_id"])
    video_windowed_stream = win.fold_window(
        "video_windowed_stream", video_keyed_stream, clock, windower, list, accumulate
    )

    video_windowed_stream = op.map("user_interaction_event", video_windowed_stream, video_interaction_event)

    # sync to feature group topic
    fg_serialized_stream = op.map(
        "video_window_agg_fg-serialize_with_key",
        video_windowed_stream,
        lambda x: serialize_with_key(x, video_window_agg_feature_group),
    )

    video_window_agg_fg_processed = op.map(
        "map", fg_serialized_stream, lambda x: sink_kafka(x[0], x[1], video_window_agg_feature_group)
    )

    #######################
    kop.output(
        "interactions_fg-kafka-out",
        processed,
        brokers=kafka_config["bootstrap.servers"],
        topic=feature_group._online_topic_name,
        add_config=kafka_config,
    )

    kop.output(
        "video_window_agg_fg-kafka-out",
        video_window_agg_fg_processed,
        brokers=kafka_config["bootstrap.servers"],
        topic=video_window_agg_feature_group._online_topic_name,
        add_config=kafka_config,
    )
    ###########################################################

    return flow
