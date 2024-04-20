package ai.hopsworks.tutorials.flink.tiktok.features;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserEngagementProcessWindow extends ProcessWindowFunction<UserWindowAggregationSchema, UserWindowAggregationSchema, String, TimeWindow> {

  @Override
  public void process(String userId, ProcessWindowFunction<UserWindowAggregationSchema, UserWindowAggregationSchema,
          String, TimeWindow>.Context context, Iterable<UserWindowAggregationSchema> iterable, Collector<UserWindowAggregationSchema> collector) {

    UserWindowAggregationSchema record = iterable.iterator().next();

    // window end
    record.setWindowEndTime(context.window().getEnd()  * 1000);

    // here it ends
    collector.collect(record);
  }
}
