package com.example.data.elasticsearch.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.serialization.Serializer;


/**
 * KafkaSerializer.
 *
 * @author gao jx
 */
public class KafkaSerializer implements Serializer<Object> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void configure(final Map<String, ?> map, final boolean b) {

    }

    @Override
    public byte[] serialize(final String s, final Object o) {
        byte[] bytes = null;
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(o);
            objectOutputStream.flush();
            objectOutputStream.close();
            bytes = byteArrayOutputStream.toByteArray();
            logger.debug(bytes.toString());
        }
        catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return bytes;
    }

    @Override
    public void close() {

    }
}
