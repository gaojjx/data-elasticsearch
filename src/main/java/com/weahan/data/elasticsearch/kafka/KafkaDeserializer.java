package com.weahan.data.elasticsearch.kafka;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.serialization.Deserializer;

/**
 * KafkaDeserializer.
 *
 * @author gao jx
 */
public class KafkaDeserializer implements Deserializer<Object> {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void configure(final Map<String, ?> map, final boolean b) {

    }

    @Override
    public Object deserialize(final String s, final byte[] bytes) {
        Object object = null;
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        try {
            final ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            object = objectInputStream.readObject();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        logger.error("topic: " + s, object);
        return object;
    }

    @Override
    public void close() {

    }
}
