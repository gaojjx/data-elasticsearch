package com.weahan.data.elasticsearch.kafka;

import java.io.Serializable;

/**
 * KafkaEsModel.
 *
 * @author gao jx
 */
public class KafkaEsModel implements Serializable {
    private String index;

    private String type;

    private String clusterName;

    private String id;

    private String uri;

    private String jsonValue;

    public KafkaEsModel() {
    }

    public KafkaEsModel(final String id, final String index, final String type, final String clusterName, final String uri) {
        this.id = id;
        this.index = index;
        this.type = type;
        this.clusterName = clusterName;
        this.uri = uri;
    }

    public final String getJsonValue() {
        return jsonValue;
    }

    public final void setJsonValue(final String jsonValue) {
        this.jsonValue = jsonValue;
    }

    public final String getIndex() {
        return index;
    }

    public final void setIndex(final String index) {
        this.index = index;
    }

    public final String getType() {
        return type;
    }

    public final void setType(final String type) {
        this.type = type;
    }

    public final String getClusterName() {
        return clusterName;
    }

    public final void setClusterName(final String clusterName) {
        this.clusterName = clusterName;
    }

    public final String getId() {
        return id;
    }

    public final void setId(final String id) {
        this.id = id;
    }

    public final String getUri() {
        return uri;
    }

    public final void setUri(final String uri) {
        this.uri = uri;
    }

    @Override
    public String toString() {
        return "KafkaEsModel{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", id='" + id + '\'' +
                ", uri='" + uri + '\'' +
                ", jsonValue='" + jsonValue + '\'' +
                '}';
    }
}
