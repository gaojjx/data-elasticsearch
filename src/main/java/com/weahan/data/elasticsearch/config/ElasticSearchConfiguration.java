package com.weahan.data.elasticsearch.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.annotation.Configuration;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * ElasticsearchConfig.
 *
 * @author gao jx
 */
@Configuration
public class ElasticSearchConfiguration extends AbstractFactoryBean {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchConfiguration.class);
    @Value("${spring.data.elasticsearch.cluster-nodes}")
    private String clusterNodes;
    @Value("${spring.data.elasticsearch.cluster-name}")
    private String clusterName;
    private RestHighLevelClient restHighLevelClient;
    static final String COLON = ":";
    static final String COMMA = ",";

    @Override
    public void destroy() {
        try {
            if (restHighLevelClient != null) {
                restHighLevelClient.close();
            }
        } catch (final Exception e) {
            LOG.error("Error closing ElasticSearch client: ", e);
        }
    }

    @Override
    public Class<RestHighLevelClient> getObjectType() {
        return RestHighLevelClient.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public RestHighLevelClient createInstance() {
        return buildClient();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
    }

    /**
     * 读取配置文件创建es连接.
     * @return
     */
    private RestHighLevelClient buildClient() {
        final String[] clusterNodeArray = clusterNodes.split(COMMA);
        final int length = clusterNodeArray.length;
        HttpHost[] hosts = new HttpHost[length];
        for (int i = 0; i < length; i++) {
            String clusterNode = clusterNodeArray[i];
            hosts[i] = HttpHost.create(clusterNode);
        }
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(hosts));
        return this.restHighLevelClient;
    }

}