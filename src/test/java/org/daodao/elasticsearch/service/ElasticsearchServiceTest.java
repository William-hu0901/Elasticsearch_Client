package org.daodao.elasticsearch.service;

import org.daodao.elasticsearch.Constants;
import org.daodao.elasticsearch.model.SampleData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ElasticsearchService
 */
class ElasticsearchServiceTest {
    
    private ElasticsearchService service;
    
    @BeforeEach
    void setUp() {
        service = new ElasticsearchService();
        // Initialize index for each test to ensure clean state
        try {
            // Delete index if it exists to start with a clean state
            try {
                service.deleteIndex(Constants.SAMPLE_INDEX_NAME);
            } catch (Exception e) {
                // Index might not exist, ignore
            }
            service.initializeIndex();
        } catch (Exception e) {
            fail("Failed to set up test environment: " + e.getMessage());
        }
    }
    
    @AfterEach
    void tearDown() {
        service.close();
    }
    
    @Test
    void testInitializeIndex() {
        assertDoesNotThrow(() -> {
            service.initializeIndex();
        });
    }
    
    @Test
    void testInsertAndRetrieveDocument() throws IOException {
        // Insert a document
        SampleData testData = new SampleData("test-item", "Test item for unit test");
        String id = service.insertDocument(testData);
        
        assertNotNull(id);
        assertFalse(id.isEmpty());
        
        // Retrieve the document
        SampleData retrievedData = service.getDocument(id);
        
        assertNotNull(retrievedData);
        assertEquals("test-item", retrievedData.getName());
        assertEquals("Test item for unit test", retrievedData.getDescription());
        assertEquals(id, retrievedData.getId());
    }
    
    @Test
    void testUpdateDocument() throws IOException {
        // Insert a document
        SampleData testData = new SampleData("update-test", "Original description");
        String id = service.insertDocument(testData);
        
        // Update the document
        SampleData updatedData = new SampleData("update-test", "Updated description");
        boolean result = service.updateDocument(id, updatedData);
        
        assertTrue(result);
        
        // Verify the update
        SampleData retrievedData = service.getDocument(id);
        assertEquals("Updated description", retrievedData.getDescription());
    }
    
    @Test
    void testDeleteDocument() throws IOException {
        // Insert a document
        SampleData testData = new SampleData("delete-test", "Item to delete");
        String id = service.insertDocument(testData);
        
        // Delete the document
        boolean result = service.deleteDocument(id);
        
        assertTrue(result);
        
        // Verify deletion
        SampleData retrievedData = service.getDocument(id);
        assertNull(retrievedData);
    }
    
    @Test
    void testSearchDocumentsByName() throws IOException {
        // Insert test data
        service.insertDocument(new SampleData("search-test-1", "First search item"));
        service.insertDocument(new SampleData("search-test-2", "Second search item"));
        service.insertDocument(new SampleData("other-item", "Non-matching item"));
        
        // Add a small delay to ensure documents are indexed
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Search for documents
        List<SampleData> results = service.searchDocumentsByName("search-test");
        
        assertEquals(2, results.size());
        assertTrue(results.stream().anyMatch(d -> "search-test-1".equals(d.getName())));
        assertTrue(results.stream().anyMatch(d -> "search-test-2".equals(d.getName())));
    }
    
    @Test
    void testGetAllDocuments() throws IOException {
        // Insert some test documents
        service.insertDocument(new SampleData("get-all-test-1", "First item"));
        service.insertDocument(new SampleData("get-all-test-2", "Second item"));
        
        // Add a small delay to ensure documents are indexed
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Get all documents
        List<SampleData> allDocuments = service.getAllDocuments();
        
        // Should have the sample data plus the two we just inserted
        assertTrue(allDocuments.size() >= 2);
    }
}