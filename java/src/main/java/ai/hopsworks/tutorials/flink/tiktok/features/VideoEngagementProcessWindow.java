package ai.hopsworks.tutorials.flink.tiktok.features;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class VideoEngagementProcessWindow
        extends ProcessWindowFunction<VideoWindowAggregationSchema, VideoWindowAggregationSchema, String, TimeWindow> {

  public VideoEngagementProcessWindow() {
  }
  @Override
  public void process(String videoId, ProcessWindowFunction<VideoWindowAggregationSchema,
          VideoWindowAggregationSchema, String, TimeWindow>.Context context,
                      Iterable<VideoWindowAggregationSchema> iterable,
                      Collector<VideoWindowAggregationSchema> collector) throws Exception {
    VideoWindowAggregationSchema record = iterable.iterator().next();
    record.setWindowEndTime(context.window().getEnd()  * 1000);
    collector.collect(record);
  }
}
