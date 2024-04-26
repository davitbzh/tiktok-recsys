package ai.hopsworks.tutorials.flink.tiktok.simulators;

import ai.hopsworks.tutorials.flink.tiktok.utils.TikTokInteractions;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class InteractionsGenerator implements GeneratorFunction<Long, TikTokInteractions> {

    private final Random randomNumber = new Random();

    private final List<String> interactionTypes = Arrays.asList("like", "view", "dislike", "comment", "share", "skip");
    private final List<Long> videoCategories = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);

    private final List<String[]> interactionIds;

    SimpleDateFormat monthFormat = new SimpleDateFormat("yyyy-MM");


    public InteractionsGenerator() throws IOException {
        this.interactionIds = readInteractionIds();

    }
    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        GeneratorFunction.super.open(readerContext);
    }

    @Override
    public void close() throws Exception {
        GeneratorFunction.super.close();
    }

    @Override
    public TikTokInteractions map(Long aLong) throws Exception {
        return interactionEventGenerator(interactionIdsGenerator(),
                videoCategoryTypeGenerator(), interactionTypeGenerator(),
                watchTimeGenerator());
    }

    private List<String[]> readInteractionIds() throws IOException {
        String interactionIdsPath = "https://repo.hops.works/dev/davit/tiktok_recsys/interaction_ids.csv";
        URL oracle = new URL(interactionIdsPath);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(oracle.openStream()));

        String inputLine;
        List<String[]> ids =  new ArrayList<>();
        int lineCount = 0;
        while ((inputLine = in.readLine()) != null && lineCount <= 1000){
            ids.add(inputLine.split(","));
            lineCount++;
        }

        in.close();
        return ids;
    }

    private String[] interactionIdsGenerator() {
        return this.interactionIds.get(randomNumber.nextInt(this.interactionIds.size()));
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

    private void timestampGenerator(TikTokInteractions tikTokInteractions){
        Long timestamp = Instant.now().toEpochMilli();
        tikTokInteractions.setInteractionDate(timestamp);
        tikTokInteractions.setInteractionMonth(this.monthFormat.format(timestamp));
    }

    private TikTokInteractions interactionEventGenerator(String[] ids, Long videoCategory, String interactionType,
                                                         Long watchTime)
    {
        TikTokInteractions tikTokInteractions = new TikTokInteractions();
        tikTokInteractions.setInteractionId(ids[0]);
        tikTokInteractions.setUserId(ids[1]);
        tikTokInteractions.setVideoId(ids[2]);
        tikTokInteractions.setCategoryId(videoCategory);
        tikTokInteractions.setInteractionType(interactionType);
        tikTokInteractions.setWatchTime(watchTime);
        timestampGenerator(tikTokInteractions);
        return tikTokInteractions;
    }
}
