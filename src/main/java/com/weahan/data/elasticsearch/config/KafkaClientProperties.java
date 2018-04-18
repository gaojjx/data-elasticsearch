package com.weahan.data.elasticsearch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * KafkaClientProperties.
 *
 * @author gao jx
 */
@ConfigurationProperties(prefix = "kafka.client")
public class KafkaClientProperties {
    private String elasticsearchSaveClient;
    private String elasticsearchUpdateClient;

    public final String getElasticsearchSaveClient() {
        return elasticsearchSaveClient;
    }

    public final void setElasticsearchSaveClient(final String elasticsearchSaveClient) {
        this.elasticsearchSaveClient = elasticsearchSaveClient;
    }

    public final String getElasticsearchUpdateClient() {
        return elasticsearchUpdateClient;
    }

    public final void setElasticsearchUpdateClient(final String elasticsearchUpdateClient) {
        this.elasticsearchUpdateClient = elasticsearchUpdateClient;
    }

    public final String getElasticsearchDeleteClient() {
        return elasticsearchDeleteClient;
    }

    public final void setElasticsearchDeleteClient(final String elasticsearchDeleteClient) {
        this.elasticsearchDeleteClient = elasticsearchDeleteClient;
    }

    private String elasticsearchDeleteClient;
}
