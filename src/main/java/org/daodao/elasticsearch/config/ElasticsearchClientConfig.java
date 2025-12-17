package org.daodao.elasticsearch.config;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.daodao.elasticsearch.util.Constants;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

/**
 * Elasticsearch Client Configuration
 */
public class ElasticsearchClientConfig {
    
    private static final Logger log = LoggerFactory.getLogger(ElasticsearchClientConfig.class);
    
    private static RestHighLevelClient client;
    
    /**
     * Get singleton instance of RestHighLevelClient
     * @return RestHighLevelClient instance
     */
    public static synchronized RestHighLevelClient getClient() {
        if (client == null) {
            client = createClient();
        }
        return client;
    }
    
    /**
     * Create RestHighLevelClient instance based on configuration
     * @return RestHighLevelClient instance
     */
    private static RestHighLevelClient createClient() {
        Properties props = loadProperties();
        
        String[] hosts = props.getProperty(Constants.ELASTICSEARCH_HOSTS, "")
                .split(",");
        
        // Set default scheme
        String scheme = props.getProperty(Constants.ELASTICSEARCH_SCHEME, 
                Constants.DEFAULT_SCHEME);
        
        HttpHost[] httpHosts = Arrays.stream(hosts)
                .filter(host -> !host.trim().isEmpty())
                .map(String::trim)
                .map(host -> parseHttpHost(host, scheme))
                .toArray(HttpHost[]::new);
        
        RestClientBuilder builder = RestClient.builder(httpHosts);
        
        // Configure timeouts
        int connectTimeout = Integer.parseInt(
                props.getProperty(Constants.ELASTICSEARCH_CONNECT_TIMEOUT, 
                        String.valueOf(Constants.DEFAULT_CONNECT_TIMEOUT)));
        int socketTimeout = Integer.parseInt(
                props.getProperty(Constants.ELASTICSEARCH_SOCKET_TIMEOUT, 
                        String.valueOf(Constants.DEFAULT_SOCKET_TIMEOUT)));
        int connectionRequestTimeout = Integer.parseInt(
                props.getProperty(Constants.ELASTICSEARCH_CONNECTION_REQUEST_TIMEOUT, 
                        String.valueOf(Constants.DEFAULT_CONNECTION_REQUEST_TIMEOUT)));
        
        builder.setRequestConfigCallback(requestConfigBuilder -> 
                requestConfigBuilder.setConnectTimeout(connectTimeout)
                        .setSocketTimeout(socketTimeout)
                        .setConnectionRequestTimeout(connectionRequestTimeout));
        
        // Scheme is set when creating HttpHost objects
        
        log.info("Creating Elasticsearch client with hosts: {}", 
                Arrays.toString(hosts));
        
        return new RestHighLevelClient(builder);
    }
    
    /**
     * Parse host string to HttpHost object
     * @param host host string in format hostname:port
     * @param scheme the protocol scheme (http or https)
     * @return HttpHost object
     */
    private static HttpHost parseHttpHost(String host, String scheme) {
        String[] parts = host.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid host format: " + host);
        }
        return new HttpHost(parts[0], Integer.parseInt(parts[1]), scheme);
    }
    
    /**
     * Load configuration properties from application.properties
     * @return Properties object
     */
    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = ElasticsearchClientConfig.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            
            if (input == null) {
                log.warn("Unable to find application.properties, using defaults");
                return props;
            }
            
            props.load(input);
        } catch (IOException e) {
            log.error("Error loading application.properties", e);
        }
        return props;
    }
    
    /**
     * Close the client and release resources
     */
    public static synchronized void closeClient() {
        if (client != null) {
            try {
                client.close();
                client = null;
                log.info("Elasticsearch client closed");
            } catch (IOException e) {
                log.error("Error closing Elasticsearch client", e);
            }
        }
    }
}