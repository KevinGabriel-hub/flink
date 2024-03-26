package org.apache.flink.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
public class Syslog {

    @JsonProperty("message_timestamp")
    String message_timestamp;

    @JsonProperty("hostname")
    String hostname;

    @JsonProperty("app")
    String app;

    @JsonProperty("ID")
    Integer id;

    @JsonProperty("ip_address")
    String ip_address;

    @JsonProperty("description")
    String description;
}
