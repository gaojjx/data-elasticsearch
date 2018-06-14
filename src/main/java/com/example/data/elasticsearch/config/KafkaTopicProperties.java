package com.example.data.elasticsearch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * KafkaTopicProperties.
 *
 * @author gao jx
 */
@Configuration
@ConfigurationProperties(prefix = "kafka.topic")
public class KafkaTopicProperties {

    private String elasticsearchSave;

    private String elasticsearchUpdate;

    private String elasticsearchDelete;

    public KafkaTopicProperties() {

    }

    public final String getElasticsearchSave() {
        return elasticsearchSave;
    }

    public final void setElasticsearchSave(final String elasticsearchSave) {
        this.elasticsearchSave = elasticsearchSave;
    }

    public final String getElasticsearchUpdate() {
        return elasticsearchUpdate;
    }

    public final void setElasticsearchUpdate(final String elasticsearchUpdate) {
        this.elasticsearchUpdate = elasticsearchUpdate;
    }

    public final String getElasticsearchDelete() {
        return elasticsearchDelete;
    }

    public final void setElasticsearchDelete(final String elasticsearchDelete) {
        this.elasticsearchDelete = elasticsearchDelete;
    }
}
