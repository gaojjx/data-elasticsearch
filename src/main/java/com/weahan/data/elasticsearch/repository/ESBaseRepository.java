package com.weahan.data.elasticsearch.repository;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

    private static final String FAIL = "fail";
    
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private ElasticsearchService elasticsearchService;

    @Autowired
    private KafkaTopicProperties kafkaTopicProperties;

    public ESBaseRepository() {
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
     * @param pageNo pageNo
     * @param pageSize pageSize
     * @return result
     */
    public String search(final String index, final QueryBuilder queryBuilder, final int pageNo, final int pageSize) {
        final SearchRequest searchRequest = new SearchRequest(index);
//        QueryBuilder queryBuilder = new WildcardQueryBuilder("author", "author");
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder).sort("id").from(pageNo).size(pageSize);
        searchRequest.source(searchSourceBuilder);
        try {
            final SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            final RestStatus status = searchResponse.status();
            final TimeValue took = searchResponse.getTook();
            final SearchHits hits = searchResponse.getHits();
            final Iterator<SearchHit> iterator = hits.iterator();
            final StringBuilder sb = new StringBuilder("[");
            while (iterator.hasNext()) {
                final SearchHit searchHit = iterator.next();
                final String sourceAsString = searchHit.getSourceAsString();
                sb.append(sourceAsString).append(",");
            }
            sb.subSequence(0, sb.length() - 1);
            sb.append("]");
            return sb.toString();
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
