package org.daodao.elasticsearch.config;

import org.daodao.elasticsearch.service.ElasticsearchService;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Simple test to verify Elasticsearch connection
 */
public class ElasticsearchConnectionTest {

    @Test
    public void testBasicConnection() {
        // Test that we can create a client without exceptions
        assertDoesNotThrow(() -> {
            RestHighLevelClient client = ElasticsearchClientConfig.getClient();
            assertNotNull(client, "Client should not be null");
        });
    }

    @Test
    public void testServiceCreation() {
        // Test that we can create the service without exceptions
        assertDoesNotThrow(() -> {
            ElasticsearchService service = new ElasticsearchService();
            assertNotNull(service, "Service should not be null");
        });
    }
}