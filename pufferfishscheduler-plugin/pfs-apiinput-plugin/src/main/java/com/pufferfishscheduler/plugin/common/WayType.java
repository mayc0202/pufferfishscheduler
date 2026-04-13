package com.pufferfishscheduler.plugin.common;

public enum WayType {
    GET("GET"),
    POST("POST");

    private String description;

    WayType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }
}
