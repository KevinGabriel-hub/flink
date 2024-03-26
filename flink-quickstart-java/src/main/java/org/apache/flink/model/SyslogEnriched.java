package org.apache.flink.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
public class SyslogEnriched {

    @JsonProperty("message_timestamp")
    String message_timestamp;

    @JsonProperty("hostname")
    String hostname;

    @JsonProperty("app")
    String app;

    @JsonProperty("ip_address_k")
    Integer ip_address_k;

    @JsonProperty("ip_address_m")
    String ip_address_m;

    @JsonProperty("hostname_m")
    String hostname_m;
}
