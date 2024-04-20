package ai.hopsworks.tutorials.flink.tiktok;

import ai.hopsworks.tutorials.flink.tiktok.features.TikTokStream;

public class TikTokFlink {
    public static void main(String[] args) throws Exception {
        new TikTokStream().stream();
    }
}
