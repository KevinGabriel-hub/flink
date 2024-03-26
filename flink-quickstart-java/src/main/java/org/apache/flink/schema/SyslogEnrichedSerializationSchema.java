package org.apache.flink.schema;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.model.SyslogEnriched;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyslogEnrichedSerializationSchema implements SerializationSchema<SyslogEnriched> {

    ObjectMapper objectMapper;
    Logger logger = LoggerFactory.getLogger(SyslogSerializationSchema.class);

    @Override
    public byte[] serialize(SyslogEnriched purchaseEnriched) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule());
        }
        try {
            return objectMapper.writeValueAsString(purchaseEnriched).getBytes();
        } catch (JsonProcessingException e) {
            logger.error("Failed to parse JSON", e);
        }
        return new byte[0];
    }
}
