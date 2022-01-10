package com.apple.customserializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CompanyDeserializer implements Deserializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, brandLen;
        String name, brand;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        brandLen = buffer.getInt();
        byte[] brandBytes = new byte[brandLen];
        buffer.get(brandBytes);

        name = new String(nameBytes, StandardCharsets.UTF_8);
        brand = new String(brandBytes, StandardCharsets.UTF_8);

        return new Company(name, brand);
    }

    @Override
    public Company deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
