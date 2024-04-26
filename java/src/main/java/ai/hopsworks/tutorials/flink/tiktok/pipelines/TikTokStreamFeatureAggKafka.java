package ai.hopsworks.tutorials.flink.tiktok.pipelines;

import ai.hopsworks.tutorials.flink.tiktok.features.*;
import ai.hopsworks.tutorials.flink.tiktok.utils.InteractionsEventKafkaSource;
import ai.hopsworks.tutorials.flink.tiktok.utils.TikTokInteractions;
import ai.hopsworks.tutorials.flink.tiktok.utils.Utils;
import com.logicalclocks.hsfs.flink.FeatureStore;
import com.logicalclocks.hsfs.flink.HopsworksConnection;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class TikTokStreamFeatureAggKafka {

  public static final int CHECKPOINTING_INTERVAL_MS = 5000;
  private static final String JOB_NAME = "TikTok Streaming Pipeline";

  private FeatureStore featureStore;

  Utils utils = new Utils();

  public TikTokStreamFeatureAggKafka() throws Exception {
    //get feature store handle
    HopsworksConnection hopsworksConnection = HopsworksConnection.builder().build();

    featureStore = hopsworksConnection.getFeatureStore();
  }

  public void stream() throws Exception {

    int parallelism = 1;
    String sourceTopic = "live_interactions";

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);

    // Setup the sliding window aggregations 5, 10, 60 minutes
    interactionSlidingWindow( env,60, 10, sourceTopic);
    //interactionSlidingWindow( env,10, 5, 1, 1);
    //interactionSlidingWindow( env,60, 10, 1, 1);

    env.execute(JOB_NAME);
    //env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
    env.setRestartStrategy(RestartStrategies.noRestart());
  }

  private void interactionSlidingWindow(StreamExecutionEnvironment env,
                                        int windowSizeMinutes,
                                        int slideSizeMinutes,
                                        String sourceTopic) throws Exception {


    // get or create stream feature group
    StreamFeatureGroup interactionsFeatureGroup = featureStore.getStreamFeatureGroup("interactions", 1);
    StreamFeatureGroup userWindowAgg = featureStore.getStreamFeatureGroup("user_window_agg_1h", 1);
    StreamFeatureGroup videoWindowAgg = featureStore.getStreamFeatureGroup("video_window_agg_1h", 1);

    WatermarkStrategy<SourceInteractions> customWatermark = WatermarkStrategy
        .<SourceInteractions>forBoundedOutOfOrderness(Duration.ofSeconds(30))
        .withTimestampAssigner((event, timestamp) -> event.getInteractionDate());

      // define transaction source
      Properties kafkaConfig = utils.getKafkaProperties(sourceTopic);

      KafkaSource<SourceInteractions> interactionsSource = KafkaSource.<SourceInteractions>builder()
              .setProperties(kafkaConfig)
              .setTopics(sourceTopic)
              .setStartingOffsets(OffsetsInitializer.latest())
              .setDeserializer(new InteractionsEventKafkaSource())
              .build();

      DataStream<SourceInteractions> interactionEvents = env.fromSource(interactionsSource, customWatermark, "Interactions Kafka Source")
              .rescale()
              .rebalance();

      // define feature aggregate streams
      DataStream<SourceInteractions> interactions =
              interactionEvents
                      .keyBy(SourceInteractions::getUserId)
                      .map((MapFunction<SourceInteractions, SourceInteractions>) source -> {
                        source.setInteractionDate(source.getInteractionDate() * 1000);
                        return source;
                      });

      DataStream<UserWindowAggregationSchema> userAggregationStream =
              interactionEvents
                      .keyBy(SourceInteractions::getUserId)
                      .map((MapFunction<SourceInteractions, TikTokInteractions>) source -> {
                        TikTokInteractions tikTokInteractions = new TikTokInteractions();
                        tikTokInteractions.setInteractionId(String.valueOf(source.getInteractionId()));
                        tikTokInteractions.setUserId(String.valueOf(source.getUserId()));
                        tikTokInteractions.setVideoId(String.valueOf(source.getVideoId()));
                        tikTokInteractions.setCategoryId(source.getCategoryId());
                        tikTokInteractions.setInteractionType(String.valueOf(source.getInteractionType()));
                        tikTokInteractions.setInteractionDate(source.getInteractionDate());
                        tikTokInteractions.setInteractionMonth(String.valueOf(source.getInteractionMonth()));
                        tikTokInteractions.setWatchTime(source.getWatchTime());
                        return tikTokInteractions;
                      })
            .keyBy(TikTokInteractions::getUserId)
            .window(SlidingEventTimeWindows.of(Time.minutes(windowSizeMinutes), Time.minutes(slideSizeMinutes)))
            .aggregate(new UserEngagementAggregation(), new UserEngagementProcessWindow());

      DataStream<VideoWindowAggregationSchema> videoAggregationStream =
              interactionEvents
                      .keyBy(SourceInteractions::getVideoId)
                      .map((MapFunction<SourceInteractions, TikTokInteractions>) source -> {
                          TikTokInteractions tikTokInteractions = new TikTokInteractions();
                          tikTokInteractions.setInteractionId(String.valueOf(source.getInteractionId()));
                          tikTokInteractions.setUserId(String.valueOf(source.getUserId()));
                          tikTokInteractions.setVideoId(String.valueOf(source.getVideoId()));
                          tikTokInteractions.setCategoryId(source.getCategoryId());
                          tikTokInteractions.setInteractionType(String.valueOf(source.getInteractionType()));
                          tikTokInteractions.setInteractionDate(source.getInteractionDate());
                          tikTokInteractions.setInteractionMonth(String.valueOf(source.getInteractionMonth()));
                          tikTokInteractions.setWatchTime(source.getWatchTime());
                          return tikTokInteractions;
                      })
            .keyBy(TikTokInteractions::getVideoId)
            .window(SlidingEventTimeWindows.of(Time.minutes(windowSizeMinutes), Time.minutes(slideSizeMinutes)))
            .aggregate(new VideoEngagementAggregation(), new VideoEngagementProcessWindow());

    // insert streams
    interactionsFeatureGroup.insertStream(interactions);
    userWindowAgg.insertStream(userAggregationStream);
    videoWindowAgg.insertStream(videoAggregationStream);
  }
}
