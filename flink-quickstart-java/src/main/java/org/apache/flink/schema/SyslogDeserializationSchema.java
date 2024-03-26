package org.apache.flink.schema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.model.Syslog;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

public class SyslogDeserializationSchema implements
        DeserializationSchema<Syslog> {

    static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public Syslog deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Syslog.class);
    }

    @Override
    public boolean isEndOfStream(Syslog product) {
        return false;
    }

    @Override
    public TypeInformation<Syslog> getProducedType() {
        return TypeInformation.of(Syslog.class);
    }
}
