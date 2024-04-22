package ai.hopsworks.tutorials.flink.tiktok.features;

import ai.hopsworks.tutorials.flink.tiktok.utils.TikTokInteractions;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class InteractionsEventsSimulator extends RichParallelSourceFunction<TikTokInteractions> {

    private final Random randomNumber = new Random();
    private final Random randomLetter = new Random();

    private final List<String> interactionTypes = Arrays.asList("like", "view", "dislike", "comment", "share", "skip");
    private final List<Long> videoCategories = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
    public static final int SLEEP_MILLIS_PER_EVENT = 5;
    private volatile boolean running = true;

    private long eventTime;

    private TikTokInteractions event;
    private int batchSize;

    public InteractionsEventsSimulator(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void run(SourceContext<TikTokInteractions> sourceContext) throws Exception {
        long id = 0;
        long maxStartTime = 0;

        while (running) {
            // generate a batch of events
            List<TikTokInteractions> events = new ArrayList<TikTokInteractions>(batchSize);
            for (int i = 1; i <= batchSize; i++) {
                eventTime = Instant.now().toEpochMilli();
                event = interactionEventGenerator(interactionIdGenerator(), userIdGenerator(), videoIdGenerator(),
                        videoCategoryTypeGenerator(), interactionTypeGenerator(), watchTimeGenerator(), eventTime,
                        timestampMonthGenerator(eventTime));
                events.add(event);
            }

            events
                    .iterator()
                    .forEachRemaining(r -> sourceContext.collectWithTimestamp(event, eventTime));

            // produce a Watermark
            sourceContext.emitWatermark(new Watermark(maxStartTime));

            // prepare for the next batch
            id += batchSize;

            // don't go too fast
            Thread.sleep(SLEEP_MILLIS_PER_EVENT); //BATCH_SIZE * SLEEP_MILLIS_PER_EVENT
        }
    }

    @Override
    public void cancel() {
    }

    private String userIdGenerator() {
        int min = 0;
        int max = 9;

        return String.format("%s%s%d%d%d%s",
                String.valueOf((char) ('a' + randomLetter.nextInt(26))).toUpperCase(),
                String.valueOf((char) ('a' + randomLetter.nextInt(26))).toUpperCase(),
                randomNumber.nextInt(max - min) + min,
                randomNumber.nextInt(max - min) + min,
                randomNumber.nextInt(max - min) + min,
                String.valueOf((char) ('a' + randomLetter.nextInt(26))).toUpperCase()
        );
    }

    private String videoIdGenerator() {
        int min = 0;
        int max = 9;

        return String.format("%d%s%s%d%d%s",
                randomNumber.nextInt(max - min) + min,
                String.valueOf((char) ('a' + randomLetter.nextInt(26))).toUpperCase(),
                String.valueOf((char) ('a' + randomLetter.nextInt(26))).toUpperCase(),
                randomNumber.nextInt(max - min) + min,
                randomNumber.nextInt(max - min) + min,
                String.valueOf((char) ('a' + randomLetter.nextInt(26))).toUpperCase()
        );
    }

    private String interactionTypeGenerator() {
        return interactionTypes.get(randomNumber.nextInt(interactionTypes.size()));
    }

    private Long videoCategoryTypeGenerator() {
        return videoCategories.get(randomNumber.nextInt(interactionTypes.size()));
    }

    private Long watchTimeGenerator() {
        long leftLimit = 10L;
        long rightLimit = 250;
        return leftLimit + (long) (Math.random() * (rightLimit - leftLimit));
    }

    private String interactionIdGenerator() {

        int min = 0;
        int max = 9;

        return String.format("%d%d%d%d-%d%d-%d%d%d%d",
                randomNumber.nextInt(max - min) + min,
                randomNumber.nextInt(max - min) + min,
                randomNumber.nextInt(max - min) + min,
                randomNumber.nextInt(max - min) + min,

                randomNumber.nextInt(max - min) + min,
                randomNumber.nextInt(max - min) + min,

                randomNumber.nextInt(max - min) + min,
                randomNumber.nextInt(max - min) + min,
                randomNumber.nextInt(max - min) + min,
                randomNumber.nextInt(max - min) + min
        );

    }

    private String timestampMonthGenerator(long timestamp){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM");
        return dateFormat.format(timestamp);
    }

    private TikTokInteractions interactionEventGenerator(String interactionId, String userId, String videoId,
                                                         Long videoCategory, String interactionType, Long watchTime,
                                                         Long interactionDate, String interactionMonth) {
        TikTokInteractions tikTokInteractions = new TikTokInteractions();
        tikTokInteractions.setInteractionId(interactionId);
        tikTokInteractions.setUserId(userId);
        tikTokInteractions.setVideoId(videoId);
        tikTokInteractions.setCategoryId(videoCategory);
        tikTokInteractions.setInteractionType(interactionType);
        tikTokInteractions.setInteractionDate(interactionDate);
        tikTokInteractions.setInteractionMonth(interactionMonth);
        tikTokInteractions.setWatchTime(watchTime);
        return tikTokInteractions;
    }
}
