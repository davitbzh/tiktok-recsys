package ai.hopsworks.tutorials.flink.tiktok.features;

import ai.hopsworks.tutorials.flink.tiktok.utils.TikTokInteractions;
import com.logicalclocks.hsfs.flink.FeatureStore;
import com.logicalclocks.hsfs.flink.HopsworksConnection;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class TikTokStream {

  public static final int CHECKPOINTING_INTERVAL_MS = 5000;
  private static final String JOB_NAME = "TikTok Streaming Pipeline";

  private FeatureStore featureStore;

  public TikTokStream() throws Exception {
    //get feature store handle
    HopsworksConnection hopsworksConnection = HopsworksConnection.builder().build();

    featureStore = hopsworksConnection.getFeatureStore();
  }

  public void stream() throws Exception {
    int parallelism = 10;
    int batchSize = 5;
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);

    // Setup the sliding window aggregations 5, 10, 60 minutes
    interactionSlidingWindow( env,2, 1, parallelism, batchSize);
    //interactionSlidingWindow( env,10, 5, 1, 1);
    //interactionSlidingWindow( env,60, 10, 1, 1);

    env.execute(JOB_NAME);
    env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
    env.setRestartStrategy(RestartStrategies.noRestart());
  }

  private void interactionSlidingWindow( StreamExecutionEnvironment env,
                                        int windowSizeMinutes,
                                        int slideSizeMinutes,
                                        int batchSize,
                                        int parallelism) throws Exception {


    // get or create stream feature group
    StreamFeatureGroup interactionsFeatureGroup = featureStore.getStreamFeatureGroup("interactions", 1);
    StreamFeatureGroup userWindowAgg = featureStore.getStreamFeatureGroup("user_window_agg_1h", 1);
    StreamFeatureGroup videoWindowAgg = featureStore.getStreamFeatureGroup("video_window_agg_1h", 1);


    WatermarkStrategy<TikTokInteractions> customWatermark = WatermarkStrategy
        .<TikTokInteractions>forBoundedOutOfOrderness(Duration.ofSeconds(30))
        .withTimestampAssigner((event, timestamp) -> event.getInteractionDate());

    DataStream<TikTokInteractions> simEvents =
            env.addSource(new InteractionsEventsSimulator(batchSize));

    simEvents.assignTimestampsAndWatermarks(customWatermark)
            .setParallelism(parallelism)
            .rescale()
            .rebalance();

    // define feature aggregate streams
    DataStream<SourceInteractions> sourceInteractions =
            simEvents
            .keyBy(TikTokInteractions::getUserId).map(new MapFunction<TikTokInteractions, SourceInteractions>() {
                      @Override
                      public SourceInteractions map(TikTokInteractions tikTokInteractions) throws Exception {
                        SourceInteractions sourceInteractions = new SourceInteractions();
                        sourceInteractions.setInteractionId(tikTokInteractions.getInteractionId());
                        sourceInteractions.setUserId(tikTokInteractions.getUserId());
                        sourceInteractions.setVideoId(tikTokInteractions.getVideoId());
                        sourceInteractions.setVideoCategory(tikTokInteractions.getVideoCategory());
                        sourceInteractions.setInteractionType(tikTokInteractions.getInteractionType());
                        sourceInteractions.setInteractionDate(tikTokInteractions.getInteractionDate() * 1000);
                        sourceInteractions.setInteractionDay(tikTokInteractions.getInteractionDay());
                        sourceInteractions.setWatchTime(tikTokInteractions.getWatchTime());
                        return sourceInteractions;
                      }
                    });

    DataStream<UserWindowAggregationSchema> userAggregationStream =
            simEvents.assignTimestampsAndWatermarks(customWatermark)
            .keyBy(TikTokInteractions::getUserId)
            .window(SlidingEventTimeWindows.of(Time.minutes(windowSizeMinutes), Time.minutes(slideSizeMinutes)))
            .aggregate(new UserEngagementAggregation(), new UserEngagementProcessWindow());

    DataStream<VideoWindowAggregationSchema> videoAggregationStream =
            simEvents.assignTimestampsAndWatermarks(customWatermark)
            .keyBy(TikTokInteractions::getVideoId)
            .window(SlidingEventTimeWindows.of(Time.minutes(windowSizeMinutes), Time.minutes(slideSizeMinutes)))
            .aggregate(new VideoEngagementAggregation(), new VideoEngagementProcessWindow());

    // insert stream
    interactionsFeatureGroup.insertStream(sourceInteractions);
    userWindowAgg.insertStream(userAggregationStream);
    videoWindowAgg.insertStream(videoAggregationStream);
  }
}
