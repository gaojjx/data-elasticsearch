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
}
