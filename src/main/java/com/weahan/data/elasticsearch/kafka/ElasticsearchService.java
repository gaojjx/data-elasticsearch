package com.weahan.data.elasticsearch.kafka;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * ElasticsearchService.
 *
 * @author gao jx
 */
@Service
public class ElasticsearchService {
    private static final String COLON = ":";

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public ElasticsearchService(final KafkaTemplate<String, Object> kafkaTemplateIn) {
        this.kafkaTemplate = kafkaTemplateIn;
    }


    /**
     * 把需要操作ES的model发送到kafka队列当中.
     *
     * @param topic topic
     * @param model model
     * @return
     */
    public String sendKafka(final String topic, final KafkaEsModel model) {
        final StringBuilder sb = new StringBuilder(model.getClusterName())
                .append(this.COLON).append(model.getIndex())
                .append(this.COLON).append(model.getType())
                .append(this.COLON).append(model.getId());
        final String key = sb.toString();
        final ListenableFuture<SendResult<String, Object>> result = kafkaTemplate.send(topic, key, model);
        try {
            final SendResult<String, Object> sendResult = result.get();
            final long offset = sendResult.getRecordMetadata().offset();
            if (offset >= 0) {
                return "success";
            }
            else {
                logger.error("kafka offsetIndex error{}" + model);
                return "fail";
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        catch (ExecutionException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return "fail";
    }

}
