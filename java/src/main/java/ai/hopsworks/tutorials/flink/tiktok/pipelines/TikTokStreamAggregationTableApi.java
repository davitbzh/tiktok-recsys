package ai.hopsworks.tutorials.flink.tiktok.pipelines;

import ai.hopsworks.tutorials.flink.tiktok.simulators.InteractionsGenerator;
import ai.hopsworks.tutorials.flink.tiktok.utils.TikTokInteractions;
import ai.hopsworks.tutorials.flink.tiktok.utils.TikTokInteractionsInst;
import com.logicalclocks.hsfs.flink.FeatureStore;
import com.logicalclocks.hsfs.flink.HopsworksConnection;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TikTokStreamAggregationTableApi {

    public static final int CHECKPOINTING_INTERVAL_MS = 5000;
    private static final String JOB_NAME = "TikTok Streaming Pipeline";

    private FeatureStore featureStore;

    public TikTokStreamAggregationTableApi() throws Exception {
        //get feature store handle
        //HopsworksConnection hopsworksConnection = HopsworksConnection.builder().build();
        /*
        HopsworksConnection hopsworksConnection = HopsworksConnection.builder()
                .host("93e3c930-5dfe-11ef-9746-974f83a27861.cloud.hopsworks.ai") // DNS of your Feature Store instance
                .port(443)                                // Port to reach your Hopsworks instance, defaults to 443
                .project("tiktok")                        // Name of your Hopsworks Feature Store project
                .apiKeyValue("8d54GZvCvz5jJsCA.43dRVf9mXJrFQJelUe1ed4e6DfcXi1HdzIKAkxvZAcb8ZPmQg1XQ9QmtD4FBzJXk")                   // The API key to authenticate with the feature store
                .hostnameVerification(false)               // Disable for self-signed certificates
                .build();

        featureStore = hopsworksConnection.getFeatureStore();
        */
    }


    public void stream(Long maxId, Long recordsPerSecond, Integer parallelism) throws Exception {


        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        // Set the modified configuration as the global configuration
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(parallelism);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Define time for start
        Instant now = Instant.now();
        // Subtract 2 weeks from the current instant
        Instant startTime = now.minus(7, ChronoUnit.DAYS);

        /*
        // get or create stream feature group
        StreamFeatureGroup interactionsFeatureGroup = featureStore.getStreamFeatureGroup("interactions", 1);
        StreamFeatureGroup userWindowAgg = featureStore.getStreamFeatureGroup("user_window_agg_1h", 1);
        StreamFeatureGroup videoWindowAgg = featureStore.getStreamFeatureGroup("video_window_agg_1h", 1);
         */

        WatermarkStrategy<TikTokInteractions> customWatermark = WatermarkStrategy
                .<TikTokInteractions>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((event, timestamp) -> event.getInteractionDate());

        DataGeneratorSource<TikTokInteractions> generatorSource =
                new DataGeneratorSource<>(
                        new InteractionsGenerator(maxId, startTime),
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(recordsPerSecond),
                        TypeInformation.of(TikTokInteractions.class));

        DataStream<TikTokInteractions> interactions =
                env.fromSource(generatorSource,
                                WatermarkStrategy.noWatermarks(),
                                "Generator Source")
                        .setParallelism(parallelism)
                        .rescale()
                        .rebalance();

        DataStream<TikTokInteractionsInst> interactionsInt = interactions.map(new MapFunction<TikTokInteractions, TikTokInteractionsInst>() {
            @Override
            public TikTokInteractionsInst map(TikTokInteractions tikTokInteractions) throws Exception {
                TikTokInteractionsInst result =  new TikTokInteractionsInst();
                result.setInteractionId(tikTokInteractions.getInteractionId());
                result.setUserId(tikTokInteractions.getUserId());
                result.setVideoId(tikTokInteractions.getVideoId());
                result.setCategoryId(tikTokInteractions.getCategoryId());
                result.setInteractionType(tikTokInteractions.getInteractionType());
                result.setInteractionDate(Instant.ofEpochSecond(tikTokInteractions.getInteractionDate()));
                result.setInteractionMonth(tikTokInteractions.getInteractionMonth());
                result.setWatchTime(tikTokInteractions.getWatchTime());
                return result;
            }
        });

        Schema schema = Schema.newBuilder()
                .column("interactionId", DataTypes.BIGINT())                         // Long
                .column("userId", DataTypes.BIGINT())                                // Long
                .column("videoId", DataTypes.BIGINT())                               // Long
                .column("categoryId", DataTypes.BIGINT())                            // Long
                .column("interactionType", DataTypes.STRING())                       // String
                .column("watchTime", DataTypes.BIGINT())                             // Long
                .column("interactionDate", DataTypes.TIMESTAMP_LTZ(3))      // Long
                .column("interactionMonth", DataTypes.STRING())                      // String
                .column("processStart", DataTypes.BIGINT())                          // Long
                .watermark("interactionDate", "interactionDate - INTERVAL '5' SECOND")
                .build();

        // Define the Flink Table Schema
        // Convert DataStream to Table
        Table interactionsSourceTable = tableEnv.fromDataStream(
                interactionsInt,
                schema
        );

        // Register the Table
        tableEnv.createTemporaryView("interactions", interactionsSourceTable);

        Table interactionCountH = tableEnv.sqlQuery(
                "SELECT " +
                        "    videoId, " +
                        "    COUNT(*) OVER (PARTITION BY videoId ORDER BY interactionDate RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS interaction_len_h, " +
                        "    interactionDate " +
                        "FROM interactions");
        tableEnv.createTemporaryView("interactionCountH", interactionCountH);

        Table interactionCountD = tableEnv.sqlQuery(
                "SELECT " +
                        "    videoId, " +
                        "    COUNT(*) OVER (PARTITION BY videoId ORDER BY interactionDate RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS interaction_len_d, " +
                        "    interactionDate " +
                        "FROM interactions");
        tableEnv.createTemporaryView("interactionCountD", interactionCountD);

        Table interactionCountW = tableEnv.sqlQuery(
                "SELECT " +
                        "    videoId, " +
                        "    COUNT(*) OVER (PARTITION BY videoId ORDER BY interactionDate RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) AS interaction_len_w, " +
                        "    interactionDate " +
                        "FROM interactions");
        tableEnv.createTemporaryView("interactionCountW", interactionCountW);

        // Calculate average watch time using RANGE
        Table averageWatchTimeH = tableEnv.sqlQuery(
                "SELECT " +
                        "    videoId, " +
                        "    AVG(watchTime) OVER (PARTITION BY videoId ORDER BY interactionDate RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS average_watch_time_h, " +
                        "    interactionDate " +
                        "FROM interactions");
        tableEnv.createTemporaryView("averageWatchTimeH", averageWatchTimeH);


        Table averageWatchTimeD = tableEnv.sqlQuery(
                "SELECT " +
                        "    videoId, " +
                        "    AVG(watchTime) OVER (PARTITION BY videoId ORDER BY interactionDate RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS average_watch_time_d, " +
                        "    interactionDate " +
                        "FROM interactions");
        tableEnv.createTemporaryView("averageWatchTimeD", averageWatchTimeD);

        Table averageWatchTimeW = tableEnv.sqlQuery(
                "SELECT " +
                        "    videoId, " +
                        "    AVG(watchTime) OVER (PARTITION BY videoId ORDER BY interactionDate RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) AS average_watch_time_w, " +
                        "    interactionDate " +
                        "FROM interactions");
        tableEnv.createTemporaryView("averageWatchTimeW", averageWatchTimeW);


        // Combine the results into a single table using JOINs
        Table videoAgg = tableEnv.sqlQuery(
                "SELECT " +
                        "    h.videoId, " +
                        "    h.interactionDate AS hour_start, " +
                        "    interaction_len_h, " +
                        "    interaction_len_d, " +
                        "    interaction_len_w, " +
                        "    average_watch_time_h, " +
                        "    average_watch_time_d, " +
                        "    average_watch_time_w " +
                        "FROM interactionCountH h " +
                        "LEFT JOIN interactionCountD d ON h.videoId = d.videoId AND h.interactionDate = d.interactionDate " +
                        "LEFT JOIN interactionCountW w ON h.videoId = w.videoId AND h.interactionDate = w.interactionDate " +
                        "LEFT JOIN averageWatchTimeH aH ON h.videoId = aH.videoId AND h.interactionDate = aH.interactionDate " +
                        "LEFT JOIN averageWatchTimeD aD ON h.videoId = aD.videoId AND h.interactionDate = aD.interactionDate " +
                        "LEFT JOIN averageWatchTimeW aW ON h.videoId = aW.videoId AND h.interactionDate = aW.interactionDate");


        // Register the views
        tableEnv.createTemporaryView("video_agg", videoAgg);
        //tableEnv.createTemporaryView("user_agg", userAgg);

        // Example: Query the views
        Table result = tableEnv.sqlQuery(
                "SELECT * FROM video_agg"
        );

        // Convert the result back to a DataStream and print
        tableEnv.toDataStream(result).print();

        // Execute the Flink job
        env.execute("Flink Table API Complex Views Example");
    }
}
