package org.apache.flink.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
public class SqlTable {

    @JsonProperty("ip_address")
    private String ip_address;

    @JsonProperty("hostname")
    private String hostname;

    @JsonProperty("spi")
    private String spi;

    @JsonProperty("network")
    private String network;

    @JsonProperty("region")
    private String region;

    public SqlTable() {
        // Empty constructor
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getSpi() {
        return spi;
    }

    public void setSpi(String spi) {
        this.spi = spi;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getIp_address() {
        return ip_address;
    }

    public void setIp_address(String ip_address) {
        this.ip_address = ip_address;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlTable sqltable = (SqlTable) o;
        return Objects.equals(ip_address, sqltable.ip_address) &&
                Objects.equals(hostname, sqltable.hostname) &&
                Objects.equals(spi, sqltable.spi) &&
                Objects.equals(network, sqltable.network) &&
                Objects.equals(region, sqltable.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip_address, hostname, spi, network, region);
    }

    @Override
    public String toString() {
        return "Sqltable{" +
                ", ipAddress='" + ip_address + '\'' +
                ", hostname='" + hostname + '\'' +
                ", spi='" + spi + '\'' +
                ", network=" + network +
                ", region='" + region + '\'' +
                '}';
    }
}
