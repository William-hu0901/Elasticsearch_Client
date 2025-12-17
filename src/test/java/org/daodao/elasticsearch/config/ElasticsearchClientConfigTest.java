package org.daodao.elasticsearch.config;

import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Unit tests for ElasticsearchClientConfig focusing on cluster connection functionality
 */
class ElasticsearchClientConfigTest {

    @AfterEach
    void tearDown() {
        // Close the client after each test to free up resources
        ElasticsearchClientConfig.closeClient();
    }

    @Test
    void testGetClientWithClusterConfiguration() {
        // This test verifies that we can create a client using cluster configuration
        // without throwing exceptions
        assertDoesNotThrow(() -> {
            RestHighLevelClient client = ElasticsearchClientConfig.getClient();
            assertNotNull(client, "Client should not be null");
        });
    }

    @Test
    void testCloseClient() {
        // This test verifies that we can close the client without exceptions
        assertDoesNotThrow(() -> {
            // First get a client instance
            RestHighLevelClient client = ElasticsearchClientConfig.getClient();
            assertNotNull(client, "Client should not be null before closing");
            
            // Then close it
            ElasticsearchClientConfig.closeClient();
        });
    }
}