package com.weahan.data.elasticsearch.repository;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weahan.data.elasticsearch.config.KafkaTopicProperties;
import com.weahan.data.elasticsearch.kafka.ElasticsearchService;
import com.weahan.data.elasticsearch.kafka.KafkaEsModel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
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
public class ESBaseRepository<T, ID extends Serializable> {

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
     * @param t
     * @param id
     * @param index
     * @param type
     * @param clusterName
     * @param uri
     * @return
     */
    public String insert(T t, ID id, final String index, final String type, final String clusterName, final String uri) {
        final KafkaEsModel model = getKafkaEsModel(id, index, type, clusterName, uri);
        final String result = elasticsearchService.sendKafka(kafkaTopicProperties.getElasticsearchSave(), model);
        logger.debug(result);
        try {
            IndexRequest indexRequest = new IndexRequest(index, type, id.toString())
                    .source(objectMapper.writeValueAsBytes(t), XContentType.JSON);
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest);
            return indexResponse.getResult().name();
        }
        catch (IOException e) {
            e.printStackTrace();
            return "FAIL";
        }
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
    public String getById(ID id, final String index, final String type, final String clusterName) {
        GetRequest getRequest = new GetRequest(index, type, id.toString());
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
     * @param uri
     * @return
     */
    public String deleteById(ID id, final String index, final String type, final String clusterName, final String uri) {
        final KafkaEsModel model = getKafkaEsModel(id, index, type, clusterName, uri);
        final String result = elasticsearchService.sendKafka(kafkaTopicProperties.getElasticsearchDelete(), model);
        logger.debug(result);
        if (!SUCCESS.equalsIgnoreCase(result)) {
            return FAIL;
        }
        DeleteRequest request = new DeleteRequest(index, type, id.toString());
        try {
            DeleteResponse response = restHighLevelClient.delete(request);
            return response.getResult().getLowercase();
        }
        catch (IOException e) {
            e.printStackTrace();
            return FAIL;
        }
    }

    private KafkaEsModel getKafkaEsModel(final ID id, final String index, final String type, final String clusterName, final String uri) {
        final KafkaEsModel model = new KafkaEsModel();
        model.setClusterName(clusterName);
        model.setId(id.toString());
        model.setIndex(index);
        model.setType(type);
        model.setUri(uri);
        return model;
    }


    /**
     * 根据id更新es的document.
     *
     * @param t
     * @param id
     * @param index
     * @param type
     * @param clusterName
     * @param uri
     * @return
     */
    public String updateById(T t, ID id, final String index, final String type, final String clusterName, final String uri) {
        final KafkaEsModel model = this.getKafkaEsModel(id, index, type, clusterName, uri);
        final String result = elasticsearchService.sendKafka(kafkaTopicProperties.getElasticsearchUpdate(), model);
        logger.debug(result);
        if (FAIL.equalsIgnoreCase(result)) {
            return FAIL;
        }
        UpdateRequest request = new UpdateRequest(index, type, id.toString());
        try {
            request.doc(objectMapper.writeValueAsString(t), XContentType.JSON);
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        try {
            UpdateResponse response = restHighLevelClient.update(request);
            return response.getResult().getLowercase();
        }
        catch (IOException e) {
            e.printStackTrace();
            return "FAIL";
        }
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
