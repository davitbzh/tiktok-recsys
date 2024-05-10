import pathway as pw

import hopsworks

from io import BytesIO
from hsfs import engine
from hsfs.feature_group import FeatureGroup


def _get_feature_group_config(feature_group):
    """
    fetches configuration for feature group online topic
    :param feature_group:
    :return:
    """

    if feature_group._kafka_producer is None:
        offline_write_options = {}  # {'internal_kafka': True}
        producer, feature_writers, writer = engine.get_instance()._init_kafka_resources(
            feature_group, offline_write_options
        )
        feature_group._kafka_producer = producer
        feature_group._feature_writers = feature_writers
        feature_group._writer = writer

    return feature_group


def construct_meta(ticker, open, high, low, close, volume, vwap, t, transactions, otc) -> bytes:
    row = dict()
    row["ticker"] = ticker
    row["open"] = open
    row["high"] = high
    row["low"] = low
    row["close"] = close
    row["volume"] = volume
    row["vwap"] = vwap
    row["t"] = t
    row["transactions"] = transactions
    row["otc"] = otc

    # encode complex features
    # row = engine.get_instance()._encode_complex_features(feature_writers, row)
    # encode feature row
    with BytesIO() as outf:
        feature_group._writer(row, outf)
        encoded_row = outf.getvalue()

    return encoded_row


def hopsworks_encode(t: pw.Table) -> bytes:

    row = pw.debug.table_to_dicts(t)

    # encode complex features
    # row = engine.get_instance()._encode_complex_features(feature_writers, row)
    # encode feature row
    with BytesIO() as outf:
        feature_group._writer(row, outf)
        encoded_row = outf.getvalue()

    return encoded_row


@pw.udf
def to_json(val) -> pw.Json:
    return pw.Json(val)


if __name__ == "__main__":
    project = hopsworks.login(
        host="edf6b960-0083-11ef-b4cf-03cc84e3dc30.cloud.hopsworks.ai",
        project="feldera",
        api_key_value="6FyL9TuGyEOsJ7Ay.4m1eza0Fie89xYiFteF6He7nQra9iI1F8I2kDY9yWyzI13nspi52z2FUg4O8ELwA")

    fs = project.get_feature_store()
    feature_group = fs.get_feature_group("ticker", 1)
    feature_group = _get_feature_group_config(feature_group)

    config = engine.get_instance()._get_kafka_config(
        feature_group.feature_store_id, {}
    )

    # https://pathway.com/developers/user-guide/exploring-pathway/from-jupyter-to-deploy/#designing-the-algorithm
    fname = "ticker.csv"
    schema = pw.schema_from_csv(fname)
    table = pw.io.csv.read(fname, schema=schema, mode="static")
    table = table.with_columns(t=table.t.dt.utc_from_timestamp(unit="ms"))

    """
    bytes_table = table.with_columns(
        projectId=str(feature_group.feature_store.project_id).encode("utf8"),
        featureGroupId=str(feature_group._id).encode("utf8"),
        subjectId=str(feature_group.subject["id"]).encode("utf8"),
        key="".join([str(pk) for pk in sorted(feature_group.primary_key)]).encode("utf8"),
        value=pw.apply(hopsworks_encode,
                       {c: to_json(pw.this[c]) for c in table.column_names()})
    )
    pw.debug.compute_and_print(bytes_table, include_id=False)
    """

    bytes_table = table.with_columns(
        projectId=str(feature_group.feature_store.project_id).encode("utf8"),
        featureGroupId=str(feature_group._id).encode("utf8"),
        subjectId=str(feature_group.subject["id"]).encode("utf8"),
        key="".join([str(pk) for pk in sorted(feature_group.primary_key)]).encode("utf8"),

        value=pw.apply(construct_meta,
                       pw.this.ticker,
                       pw.this.open,
                       pw.this.high,
                       pw.this.low,
                       pw.this.close,
                       pw.this.volume,
                       pw.this.vwap,
                       pw.this.t,
                       pw.this.transactions,
                       pw.this.otc)
    )

    pw.io.kafka.write(
        table,
        rdkafka_settings=config,
        topic_name=feature_group._online_topic_name,
        format="raw",
        key=bytes_table.key,
        value=bytes_table.value,
        headers=[bytes_table.projectId, bytes_table.featureGroupId, bytes_table.subjectId],
    )


    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
