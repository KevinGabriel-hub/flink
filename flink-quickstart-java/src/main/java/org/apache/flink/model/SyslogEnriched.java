package org.apache.flink.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
public class SyslogEnriched {

    @JsonProperty("message_timestamp")
    private String message_timestamp;

    @JsonProperty("hostname")
    private String hostname;

    @JsonProperty("app")
    private String app;

    @JsonProperty("ip_address")
    private String ip_address;

    @JsonProperty("ip_address0")
    private String ip_address0;

    @JsonProperty("hostname0")
    private String hostname0;

    public SyslogEnriched() {
        // Empty constructor
    }

    // Constructor to initialize SyslogEnriched from Syslog and SqlTable
    public SyslogEnriched(Syslog syslog, SqlTable sqlTableRow) {
        this.message_timestamp = syslog.getMessage_timestamp();
        this.hostname = syslog.getHostname();
        this.app = syslog.getApp();
        this.ip_address = syslog.getIp_address();

        // Populate fields from SqlTable
        this.ip_address0 = sqlTableRow.getIp_address();
        this.hostname0 = sqlTableRow.getHostname();
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

    public String getIp_address() {
        return ip_address;
    }

    public void setIp_address(String ip_address) {
        this.ip_address = ip_address;
    }

    public String getIp_address0() {
        return ip_address0;
    }

    public void setIp_address0(String ip_address0) {
        this.ip_address0 = ip_address0;
    }

    public String getHostname0() {
        return hostname0;
    }

    public void setHostname0(String hostname0) {
        this.hostname0 = hostname0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SyslogEnriched that = (SyslogEnriched) o;
        return Objects.equals(message_timestamp, that.message_timestamp) &&
                Objects.equals(hostname, that.hostname) &&
                Objects.equals(app, that.app) &&
                Objects.equals(ip_address, that.ip_address) &&
                Objects.equals(ip_address0, that.ip_address0) &&
                Objects.equals(hostname0, that.hostname0);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message_timestamp, hostname, app, ip_address, ip_address0, hostname0);
    }

    @Override
    public String toString() {
        return "SyslogEnriched{" +
                "messageTimestamp='" + message_timestamp + '\'' +
                ", hostname='" + hostname + '\'' +
                ", app='" + app + '\'' +
                ", ipAddressK=" + ip_address +
                ", ipAddressM='" + ip_address0 + '\'' +
                ", hostnameM='" + hostname0 + '\'' +
                '}';
    }
}
