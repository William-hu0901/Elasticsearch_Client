package org.daodao.elasticsearch.util;

/**
 * Constants for the Elasticsearch client application
 */
public final class Constants {
    
    // Private constructor to prevent instantiation
    private Constants() {
        // This class should not be instantiated
    }
    
    // Configuration property keys
    public static final String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";
    public static final String ELASTICSEARCH_CLUSTER_HOSTS = "elasticsearch.cluster.hosts";
    public static final String ELASTICSEARCH_SCHEME = "elasticsearch.scheme";
    public static final String ELASTICSEARCH_CONNECT_TIMEOUT = "elasticsearch.connectTimeout";
    public static final String ELASTICSEARCH_SOCKET_TIMEOUT = "elasticsearch.socketTimeout";
    public static final String ELASTICSEARCH_CONNECTION_REQUEST_TIMEOUT = "elasticsearch.connectionRequestTimeout";
    
    // Default values
    public static final String DEFAULT_SCHEME = "http";
    public static final int DEFAULT_CONNECT_TIMEOUT = 5000;
    public static final int DEFAULT_SOCKET_TIMEOUT = 60000;
    public static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT = 5000;
    
    // Index names
    public static final String SAMPLE_INDEX_NAME = "sample_data";
    
    // Document fields
    public static final String FIELD_ID = "id";
    public static final String FIELD_NAME = "name";
    public static final String FIELD_DESCRIPTION = "description";
    public static final String FIELD_TIMESTAMP = "timestamp";
}