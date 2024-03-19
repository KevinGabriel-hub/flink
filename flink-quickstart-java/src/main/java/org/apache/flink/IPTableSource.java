package org.apache.flink;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class IPTableSource implements SourceFunction<Tuple5<String, String, String, String, String>> {

    private static final String JDBC_URL = "jdbc:mysql://mysql:3306/flink";
    private static final String USERNAME = "flink";
    private static final String PASSWORD = "password";
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple5<String, String, String, String, String>> ctx) throws Exception {
        // Establish JDBC connection
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            String sqlQuery = "SELECT ip_address, hostname, spi, network, region FROM your_table_name";
            try (PreparedStatement statement = connection.prepareStatement(sqlQuery);
                 ResultSet resultSet = statement.executeQuery()) {
                // Fetch data from MySQL and emit tuples
                while (resultSet.next() && isRunning) {
                    String ipAddress = resultSet.getString("ip_address");
                    String hostname = resultSet.getString("hostname");
                    String spi = resultSet.getString("spi");
                    String network = resultSet.getString("network");
                    String region = resultSet.getString("region");

                    Tuple5<String, String, String, String, String> tuple =
                            Tuple5.of(ipAddress, hostname, spi, network, region);
                    ctx.collect(tuple);
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

