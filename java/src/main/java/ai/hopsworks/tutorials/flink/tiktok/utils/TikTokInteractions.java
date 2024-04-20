package ai.hopsworks.tutorials.flink.tiktok.utils;

import java.time.Instant;

public class TikTokInteractions {
    private String interactionId;
    private String userId;
    private String videoId;
    private String videoCategory;
    private String interactionType;
    private Long watchTime;
    private Long interactionDate;
    private String interactionDay;


    public void setInteractionId(String interactionId) {
        this.interactionId = interactionId;
    }

    public String getInteractionId() {
        return interactionId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserId() {
        return userId;
    }

    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }

    public String getVideoId() {
        return videoId;
    }

    public void setVideoCategory(String videoCategory) {
        this.videoCategory = videoCategory;
    }

    public String getVideoCategory() {
        return videoCategory;
    }

    public void setInteractionType(String interactionType) {
        this.interactionType = interactionType;
    }

    public String getInteractionType() {
        return interactionType;
    }

    public void setWatchTime(Long watchTime) {
        this.watchTime = watchTime;
    }

    public Long getWatchTime() {
        return watchTime;
    }

    public void setInteractionDate(Long interactionDate) {
        this.interactionDate = interactionDate;
    }

    public Long getInteractionDate() {
        return interactionDate;
    }

    public void setInteractionDay(String interactionDay) {
        this.interactionDay = interactionDay;
    }

    public String getInteractionDay() {
        return interactionDay;
    }
}
