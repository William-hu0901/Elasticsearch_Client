package org.daodao.elasticsearch.service;

import org.daodao.elasticsearch.util.Constants;
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
    private String testIndexName;
    
    @BeforeEach
    void setUp() {
        service = new ElasticsearchService();
        // Initialize a unique index for each test to ensure clean state
        try {
            service.initializeTestIndex();
            testIndexName = service.getTestIndexName();
        } catch (Exception e) {
            fail("Failed to set up test environment: " + e.getMessage());
        }
    }
    
    @AfterEach
    void tearDown() {
        // Clean up the test index
        service.deleteTestIndex();
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
        String id = service.insertDocument(testIndexName, testData);
        
        assertNotNull(id);
        assertFalse(id.isEmpty());
        
        // Retrieve the document
        SampleData retrievedData = service.getDocument(testIndexName, id);
        
        assertNotNull(retrievedData);
        assertEquals("test-item", retrievedData.getName());
        assertEquals("Test item for unit test", retrievedData.getDescription());
        assertEquals(id, retrievedData.getId());
    }
    
    @Test
    void testUpdateDocument() throws IOException {
        // Insert a document
        SampleData testData = new SampleData("update-test", "Original description");
        String id = service.insertDocument(testIndexName, testData);
        
        // Update the document
        SampleData updatedData = new SampleData("update-test", "Updated description");
        boolean result = service.updateDocument(testIndexName, id, updatedData);
        
        assertTrue(result);
        
        // Verify the update
        SampleData retrievedData = service.getDocument(testIndexName, id);
        assertEquals("Updated description", retrievedData.getDescription());
    }
    
    @Test
    void testDeleteDocument() throws IOException {
        // Insert a document
        SampleData testData = new SampleData("delete-test", "Item to delete");
        String id = service.insertDocument(testIndexName, testData);
        
        // Delete the document
        boolean result = service.deleteDocument(testIndexName, id);
        
        assertTrue(result);
        
        // Verify deletion
        SampleData retrievedData = service.getDocument(testIndexName, id);
        assertNull(retrievedData);
    }
    
    @Test
    void testSearchDocumentsByName() throws IOException {
        // Insert test data
        service.insertDocument(testIndexName, new SampleData("search-test-1", "First search item"));
        service.insertDocument(testIndexName, new SampleData("search-test-2", "Second search item"));
        service.insertDocument(testIndexName, new SampleData("other-item", "Non-matching item"));
        
        // Add a small delay to ensure documents are indexed
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Search for documents
        List<SampleData> results = service.searchDocumentsByName(testIndexName, "search-test");
        
        // Verify we have the expected results
        assertFalse(results.isEmpty(), "Should find at least one document");
        assertTrue(results.stream().allMatch(d -> d.getName().contains("search-test")), 
            "All results should have names containing 'search-test'");
        
        // Count how many documents match our criteria
        long count = results.stream().filter(d -> d.getName().startsWith("search-test-")).count();
        assertEquals(2, count, "Should find exactly 2 documents with names starting with 'search-test-'");
    }
    
    @Test
    void testGetAllDocuments() throws IOException {
        // Get initial count
        long initialCount = service.getDocumentCount(testIndexName);
        
        // Insert some test documents
        service.insertDocument(testIndexName, new SampleData("get-all-test-1", "First item"));
        service.insertDocument(testIndexName, new SampleData("get-all-test-2", "Second item"));
        
        // Add a small delay to ensure documents are indexed
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Get all documents
        List<SampleData> allDocuments = service.getAllDocuments(testIndexName);
        
        // Should have the initial count plus the two we just inserted
        assertEquals(initialCount + 2, allDocuments.size(), "Document count should increase by 2");
    }
}