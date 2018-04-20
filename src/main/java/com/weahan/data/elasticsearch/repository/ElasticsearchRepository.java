package com.weahan.data.elasticsearch.repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.weahan.data.elasticsearch.config.KafkaTopicProperties;
import com.weahan.data.elasticsearch.kafka.ElasticsearchService;
import com.weahan.data.elasticsearch.kafka.KafkaEsModel;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * TRepository.
 *
 * @author gao jx
 */
@Component
public class ElasticsearchRepository {

    private static final String FAIL = "fail";
    
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private ElasticsearchService elasticsearchService;

    @Autowired
    private KafkaTopicProperties kafkaTopicProperties;

    public ElasticsearchRepository() {
    }

    /**
     * 保存到es.
     * @param id id
     * @param index index
     * @param type type
     * @param clusterName clusterName
     * @param uri uri
     * @return result
     */
    public String save(final String id, final String index, final String type, final String clusterName, final String uri) {
        final KafkaEsModel model = new KafkaEsModel(id, index, type, clusterName, uri);
        final String result = elasticsearchService.sendKafka(kafkaTopicProperties.getElasticsearchSave(), model);
        return result;
    }

    /**
     * 根据id获取es的document.
     *
     * @param id id
     * @param index index
     * @param type type
     * @param clusterName clusterName
     * @return result
     */
    public String getById(final String id, final String index, final String type, final String clusterName) {
        final GetRequest getRequest = new GetRequest(index, type, id);
        try {
            final GetResponse getResponse = restHighLevelClient.get(getRequest);
            if (getResponse.isExists()) {
                final String jsonString = getResponse.getSourceAsString();
                return jsonString;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return FAIL;
    }

    /**
     * 根据id删除es中的document.
     *
     * @param id id
     * @param index index
     * @param type type
     * @param clusterName clusterName
     * @return result
     */
    public String deleteById(final String id, final String index, final String type, final String clusterName) {
        final KafkaEsModel model = new KafkaEsModel(clusterName, id, index, type, null);
        final String result = elasticsearchService.sendKafka(kafkaTopicProperties.getElasticsearchDelete(), model);
        logger.debug(result);
        return result;
    }

    /**
     * 根据id更新es的document.
     *
     * @param id id
     * @param index index
     * @param type type
     * @param clusterName clusterName
     * @param uri uri
     * @return result
     */
    public String updateById(final String id, final String index, final String type, final String clusterName, final String uri) {
        final KafkaEsModel model = new KafkaEsModel(id, index, type, clusterName, uri);
        final String result = elasticsearchService.sendKafka(kafkaTopicProperties.getElasticsearchUpdate(), model);
        logger.debug(result);
        return result;
    }

    /**
     * 分页查询es.
     *
     * @param index index
     * @param queryBuilder queryBuilder
     * @param pageable pageable
     * @return result
     */
    public Page<JSONObject> search(final String index, final QueryBuilder queryBuilder, final Pageable pageable) {
        final SearchRequest searchRequest = new SearchRequest(index);
        final int pageNumber = pageable.getPageNumber();
        final int pageSize = pageable.getPageSize();
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询返回字段过滤掉id
        searchSourceBuilder.fetchSource(null, "id");
        searchSourceBuilder.query(queryBuilder).sort("id").from(pageNumber).size(pageSize);
        final Page<JSONObject> page;
        searchRequest.source(searchSourceBuilder);
        try {
            final SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            final SearchHits hits = searchResponse.getHits();
            final long total = hits.getTotalHits();
            final List<JSONObject> jsonObjectList = new ArrayList<>();
            JSONObject jsonObject;
            for (final SearchHit searchHit : hits) {
                final String sourceAsString = searchHit.getSourceAsString();
                jsonObject = JSON.parseObject(sourceAsString);
                jsonObjectList.add(jsonObject);
            }
            page = new PageImpl<>(jsonObjectList, pageable, total);
            return page;
        }
        catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return new PageImpl<>(Collections.emptyList(), pageable, 0);
        }
    }

}
