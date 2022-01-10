package com.apple.customserializer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CompanySerializer implements Serializer<Company> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }

        byte[] name;
        byte[] brand;

        if (data.getName() != null) {
            name = data.getName().getBytes(StandardCharsets.UTF_8);
        } else {
            name = new byte[0];
        }

        if (data.getBrand() != null) {
            brand = data.getBrand().getBytes(StandardCharsets.UTF_8);
        } else {
            brand = new byte[0];
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + brand.length);
        buffer.putInt(name.length);
        buffer.put(name);
        buffer.putInt(brand.length);
        buffer.put(brand);

        return buffer.array();
        //return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Company data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
