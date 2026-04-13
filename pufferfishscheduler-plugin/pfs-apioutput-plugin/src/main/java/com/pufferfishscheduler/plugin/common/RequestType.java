package com.pufferfishscheduler.plugin.common;

public enum RequestType {
    APPLICATION_X_WWW_FORM_URLENCODED("application/x-www-form-urlencoded"),
    APPLICATION_XML("application/xml"),
    APPLICATION_JSON("application/json"),
    APPLICATION_WSDL_XML("application/wsdl+xml"),
    TEXT_HTML("text/html"),
    TEXT_XML("text/xml");

    private String description;

    RequestType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }

    public static RequestType getRequestTypeByDescription(String description) {
        for (RequestType type : values()) {
            if (type.getDescription().equalsIgnoreCase(description)) {
                return type;
            }
        }
        return null;
    }
}
