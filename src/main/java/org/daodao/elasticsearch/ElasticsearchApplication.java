package org.daodao.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.daodao.elasticsearch.model.SampleData;
import org.daodao.elasticsearch.service.ElasticsearchService;

import java.io.IOException;
import java.util.List;

/**
 * Main application to demonstrate Elasticsearch operations
 */
@Slf4j
public class ElasticsearchApplication {
    
    public static void main(String[] args) {
        ElasticsearchService service = new ElasticsearchService();
        
        try {
            // Initialize index and insert sample data if needed
            service.initializeIndex();
            
            // Demonstrate CRUD operations
            demonstrateCRUD(service);
            
        } catch (Exception e) {
            log.error("Error in Elasticsearch application", e);
        } finally {
            // Clean up resources
            service.close();
        }
    }
    
    /**
     * Demonstrate CRUD operations
     * @param service Elasticsearch service
     * @throws IOException if communication with Elasticsearch fails
     */
    private static void demonstrateCRUD(ElasticsearchService service) throws IOException {
        // Create - Insert a new document
        SampleData newData = new SampleData("demo-item", "Demonstration item");
        String id = service.insertDocument(newData);
        log.info("Created document with ID: {}", id);
        
        // Read - Retrieve the document
        SampleData retrievedData = service.getDocument(id);
        log.info("Retrieved document: {}", retrievedData);
        
        // Update - Modify the document
        if (retrievedData != null) {
            retrievedData.setDescription("Updated demonstration item");
            boolean updated = service.updateDocument(id, retrievedData);
            log.info("Document updated: {}", updated);
            
            // Read again to verify update
            SampleData updatedData = service.getDocument(id);
            log.info("Updated document: {}", updatedData);
        }
        
        // Search - Find documents by name
        List<SampleData> searchResults = service.searchDocumentsByName("demo");
        log.info("Found {} documents matching 'demo'", searchResults.size());
        
        // Read all documents
        List<SampleData> allDocuments = service.getAllDocuments();
        log.info("Total documents in index: {}", allDocuments.size());
        
        // Delete - Remove the document
        if (id != null) {
            boolean deleted = service.deleteDocument(id);
            log.info("Document deleted: {}", deleted);
        }
    }
}