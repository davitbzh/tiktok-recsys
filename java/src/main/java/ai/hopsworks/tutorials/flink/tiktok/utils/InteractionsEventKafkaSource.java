package ai.hopsworks.tutorials.flink.tiktok.utils;

import ai.hopsworks.tutorials.flink.tiktok.features.SourceInteractions;
import lombok.SneakyThrows;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;

public class InteractionsEventKafkaSource implements KafkaDeserializationSchema<SourceInteractions>,
        KafkaRecordDeserializationSchema<SourceInteractions> {

    private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

    private transient DescriptiveStatisticsHistogram eventTimeLag;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        KafkaRecordDeserializationSchema.super.open(context);
        eventTimeLag =
                context
                        .getMetricGroup()
                        .histogram(
                                "interactionsEventKafkaSourceLag",
                                new DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE));
    }

    @Override
    public boolean isEndOfStream(SourceInteractions sourceInteractions) {
        return false;
    }

    @Override
    public SourceInteractions deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        byte[] messageKey = consumerRecord.key();
        byte[] message = consumerRecord.value();
        long offset = consumerRecord.offset();
        long timestamp = consumerRecord.timestamp();

        SourceInteractions sourceInteractions = new SourceInteractions();
        ByteArrayInputStream in = new ByteArrayInputStream(message);
        DatumReader<SourceInteractions> userDatumReader = new SpecificDatumReader<>(sourceInteractions.getSchema());
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
        sourceInteractions = userDatumReader.read(null, decoder);
        eventTimeLag.update(Instant.now().toEpochMilli() - sourceInteractions.getInteractionDate());
        return sourceInteractions;
    }

    @SneakyThrows
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<SourceInteractions> collector)
            throws IOException {
        SourceInteractions sourceInteractions = deserialize(consumerRecord);
        collector.collect(sourceInteractions);
    }

    @Override
    public TypeInformation<SourceInteractions> getProducedType() {
        return TypeInformation.of(SourceInteractions.class);
    }
}
