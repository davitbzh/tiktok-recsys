package ai.hopsworks.tutorials.flink.tiktok;

import ai.hopsworks.tutorials.flink.tiktok.pipelines.TikTokStreamFeatureAggKafka;
import ai.hopsworks.tutorials.flink.tiktok.pipelines.TikTokStreamFeatureAggregations;

public class TikTokFlink {
    public static void main(String[] args) throws Exception {
        new TikTokStreamFeatureAggregations().stream();
    }
}
