package com.weahan.data.elasticsearch.repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weahan.data.elasticsearch.config.KafkaTopicProperties;
import com.weahan.data.elasticsearch.kafka.ElasticsearchService;
import com.weahan.data.elasticsearch.kafka.KafkaEsModel;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * TRepository.
 *
 * @author gao jx
 */
@Component
public class ESBaseRepository {

    public static final String FAIL = "fail";

    public static final String SUCCESS = "success";

    public final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ElasticsearchService elasticsearchService;

    @Autowired
    private KafkaTopicProperties kafkaTopicProperties;

    /**
     * @param id
     * @param index
     * @param type
     * @param clusterName
     * @param uri
     * @return
     */
    public String save(String id, final String index, final String type, final String clusterName, final String uri) {
        final KafkaEsModel model = getKafkaEsModel(id, index, type, clusterName, uri);
        final String result = elasticsearchService.sendKafka(kafkaTopicProperties.getElasticsearchSave(), model);
        return result;
    }

    /**
     * 根据id获取es的document.
     *
     * @param id
     * @param index
     * @param type
     * @param clusterName
     * @return
     */
    public String getById(String id, final String index, final String type, final String clusterName) {
        GetRequest getRequest = new GetRequest(index, type, id);
        try {
            GetResponse getResponse = restHighLevelClient.get(getRequest);
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
     * @param id
     * @param index
     * @param type
     * @param clusterName
     * @return
     */
    public String deleteById(String id, final String index, final String type, final String clusterName) {
        final KafkaEsModel model = getKafkaEsModel(id, index, type, clusterName, null);
        final String result = elasticsearchService.sendKafka(kafkaTopicProperties.getElasticsearchDelete(), model);
        logger.debug(result);
        return result;
    }

    private KafkaEsModel getKafkaEsModel(final String id, final String index, final String type, final String clusterName, final String uri) {
        final KafkaEsModel model = new KafkaEsModel();
        model.setClusterName(clusterName);
        model.setId(id);
        model.setIndex(index);
        model.setType(type);
        model.setUri(uri);
        return model;
    }


    /**
     * 根据id更新es的document.
     *
     * @param id
     * @param index
     * @param type
     * @param clusterName
     * @param uri
     * @return
     */
    public String updateById(String id, final String index, final String type, final String clusterName, final String uri) {
        final KafkaEsModel model = this.getKafkaEsModel(id, index, type, clusterName, uri);
        final String result = elasticsearchService.sendKafka(kafkaTopicProperties.getElasticsearchUpdate(), model);
        logger.debug(result);
        return result;
    }

    /**
     * 分页查询es.
     *
     * @param index
     * @param queryBuilder
     * @param pageNo
     * @param pageSize
     * @return
     */
    public String search(String index, QueryBuilder queryBuilder, int pageNo, int pageSize) {
        final SearchRequest searchRequest = new SearchRequest(index);
//        QueryBuilder queryBuilder = new WildcardQueryBuilder("author", "author");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder).sort("id").from(pageNo).size(pageSize);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            final RestStatus status = searchResponse.status();
            System.out.println(status);
            final TimeValue took = searchResponse.getTook();
            final SearchHits hits = searchResponse.getHits();
            final Iterator<SearchHit> iterator = hits.iterator();
            List<String> list = new ArrayList<>();
            while (iterator.hasNext()) {
                final SearchHit searchHit = iterator.next();
                final String sourceAsString = searchHit.getSourceAsString();
                list.add(sourceAsString);
            }
            return objectMapper.writeValueAsString(list);
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
