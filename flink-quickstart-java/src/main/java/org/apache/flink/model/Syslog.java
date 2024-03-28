package org.apache.flink.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
public class Syslog {

    @JsonProperty("message_timestamp")
    private String message_timestamp;

    @JsonProperty("hostname")
    private String hostname;

    @JsonProperty("app")
    private String app;

    @JsonProperty("id")
    private Integer id;

    @JsonProperty("ip_address")
    private String ip_address;

    @JsonProperty("description")
    private String description;

    public Syslog() {
        // Empty constructor
    }

    public String getMessage_timestamp() {
        return message_timestamp;
    }

    public void setMessage_timestamp(String message_timestamp) {
        this.message_timestamp = message_timestamp;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getIp_address() {
        return ip_address;
    }

    public void setIp_address(String ip_address) {
        this.ip_address = ip_address;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Syslog syslog = (Syslog) o;
        return Objects.equals(message_timestamp, syslog.message_timestamp) &&
                Objects.equals(hostname, syslog.hostname) &&
                Objects.equals(app, syslog.app) &&
                Objects.equals(id, syslog.id) &&
                Objects.equals(ip_address, syslog.ip_address) &&
                Objects.equals(description, syslog.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message_timestamp, hostname, app, id, ip_address, description);
    }

    @Override
    public String toString() {
        return "Syslog{" +
                "messageTimestamp='" + message_timestamp + '\'' +
                ", hostname='" + hostname + '\'' +
                ", app='" + app + '\'' +
                ", id=" + id +
                ", ipAddress='" + ip_address + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
