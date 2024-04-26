package ai.hopsworks.tutorials.flink.tiktok;

import ai.hopsworks.tutorials.flink.tiktok.pipelines.TikTokStreamFeatureAggKafka;

public class TikTokFlink {
    public static void main(String[] args) throws Exception {
        new TikTokStreamFeatureAggKafka().stream();
    }
}
