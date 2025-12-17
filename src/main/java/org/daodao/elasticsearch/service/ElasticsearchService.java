package org.daodao.elasticsearch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.daodao.elasticsearch.util.Constants;
import org.daodao.elasticsearch.config.ElasticsearchClientConfig;
import org.daodao.elasticsearch.model.SampleData;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;


/**
 * Service class for Elasticsearch CRUD operations
 */
public class ElasticsearchService {
    
    private static final Logger log = LoggerFactory.getLogger(ElasticsearchService.class);
    
    private final RestHighLevelClient client;
    private final ObjectMapper objectMapper;
    private String testIndexName; // For testing purposes
    
    public ElasticsearchService() {
        this.client = ElasticsearchClientConfig.getClient();
        this.objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
    }
    
    /**
     * Get the test index name (for testing purposes)
     * @return test index name
     */
    public String getTestIndexName() {
        if (testIndexName == null) {
            testIndexName = Constants.SAMPLE_INDEX_NAME + "_" + UUID.randomUUID().toString().replace("-", "");
        }
        return testIndexName;
    }
    
    /**
     * Check if index exists, if not create it with sample data
     */
    public void initializeIndex() {
        try {
            if (!indexExists(Constants.SAMPLE_INDEX_NAME)) {
                createIndex(Constants.SAMPLE_INDEX_NAME);
                insertSampleData();
            }
        } catch (IOException e) {
            log.error("Error initializing index", e);
            throw new RuntimeException("Failed to initialize index", e);
        }
    }
    
    /**
     * Initialize index for testing purposes - creates a unique index for each test
     */
    public void initializeTestIndex() {
        try {
            String indexName = getTestIndexName();
            // Always try to delete first to ensure clean state
            try {
                if (indexExists(indexName)) {
                    deleteIndex(indexName);
                    Thread.sleep(100); // Small delay to ensure deletion completes
                }
            } catch (Exception e) {
                log.debug("Could not delete existing test index, continuing anyway", e);
            }
            
            // Create the index
            createIndex(indexName);
        } catch (Exception e) {
            log.error("Error initializing test index", e);
            throw new RuntimeException("Failed to initialize test index", e);
        }
    }
    
    /**
     * Delete the test index (for testing purposes)
     */
    public void deleteTestIndex() {
        try {
            String indexName = getTestIndexName();
            if (indexExists(indexName)) {
                deleteIndex(indexName);
            }
        } catch (Exception e) {
            log.warn("Error deleting test index", e);
        }
    }
    
    /**
     * Check if index exists
     * @param indexName index name
     * @return true if index exists, false otherwise
     * @throws IOException if communication with Elasticsearch fails
     */
    public boolean indexExists(String indexName) throws IOException {
        try {
            GetIndexRequest request = new GetIndexRequest();
            request.indices(indexName);
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            // If we get an exception, the index likely doesn't exist
            log.debug("Index {} does not exist or is inaccessible", indexName);
            return false;
        }
    }
    
    /**
     * Create index with settings
     * @param indexName index name
     * @throws IOException if communication with Elasticsearch fails
     */
    public void createIndex(String indexName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        
        // Configure index settings
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1) // Use fewer shards for testing
                .put("index.number_of_replicas", 0) // Use no replicas for testing to speed up operations
        );
        
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        if (createIndexResponse.isAcknowledged()) {
            log.info("Index {} created successfully", indexName);
        } else {
            log.warn("Index {} creation was not acknowledged", indexName);
        }
    }
    
    /**
     * Delete index
     * @param indexName index name
     * @throws IOException if communication with Elasticsearch fails
     */
    public void deleteIndex(String indexName) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        if (deleteIndexResponse.isAcknowledged()) {
            log.info("Index {} deleted successfully", indexName);
        } else {
            log.warn("Index {} deletion was not acknowledged", indexName);
        }
    }
    
    /**
     * Insert sample data into the index
     * @throws IOException if communication with Elasticsearch fails
     */
    public void insertSampleData() throws IOException {
        List<SampleData> sampleDataList = List.of(
                new SampleData("item1", "First sample item"),
                new SampleData("item2", "Second sample item"),
                new SampleData("item3", "Third sample item")
        );
        
        for (SampleData data : sampleDataList) {
            insertDocument(data);
        }
        
        log.info("Inserted {} sample documents", sampleDataList.size());
    }
    
    /**
     * Insert a document into the index
     * @param data sample data to insert
     * @return document ID
     * @throws IOException if communication with Elasticsearch fails
     */
    public String insertDocument(SampleData data) throws IOException {
        return insertDocument(Constants.SAMPLE_INDEX_NAME, data);
    }
    
    /**
     * Insert a document into the specified index
     * @param indexName index name
     * @param data sample data to insert
     * @return document ID
     * @throws IOException if communication with Elasticsearch fails
     */
    public String insertDocument(String indexName, SampleData data) throws IOException {
        IndexRequest indexRequest = new IndexRequest(indexName);
        
        if (data.getId() != null && !data.getId().isEmpty()) {
            indexRequest.id(data.getId());
        }
        
        String jsonData = objectMapper.writeValueAsString(data);
        indexRequest.source(jsonData, XContentType.JSON);
        
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        log.info("Document inserted with ID: {} into index: {}", indexResponse.getId(), indexName);
        return indexResponse.getId();
    }
    
    /**
     * Get a document by ID
     * @param id document ID
     * @return SampleData object or null if not found
     * @throws IOException if communication with Elasticsearch fails
     */
    public SampleData getDocument(String id) throws IOException {
        return getDocument(Constants.SAMPLE_INDEX_NAME, id);
    }
    
    /**
     * Get a document by ID from the specified index
     * @param indexName index name
     * @param id document ID
     * @return SampleData object or null if not found
     * @throws IOException if communication with Elasticsearch fails
     */
    public SampleData getDocument(String indexName, String id) throws IOException {
        GetRequest getRequest = new GetRequest(indexName, id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        
        if (getResponse.isExists()) {
            String jsonData = getResponse.getSourceAsString();
            SampleData data = objectMapper.readValue(jsonData, SampleData.class);
            data.setId(getResponse.getId());
            return data;
        } else {
            log.warn("Document with ID {} not found in index {}", id, indexName);
            return null;
        }
    }
    
    /**
     * Update a document
     * @param id document ID
     * @param data updated data
     * @return true if successful, false otherwise
     * @throws IOException if communication with Elasticsearch fails
     */
    public boolean updateDocument(String id, SampleData data) throws IOException {
        return updateDocument(Constants.SAMPLE_INDEX_NAME, id, data);
    }
    
    /**
     * Update a document in the specified index
     * @param indexName index name
     * @param id document ID
     * @param data updated data
     * @return true if successful, false otherwise
     * @throws IOException if communication with Elasticsearch fails
     */
    public boolean updateDocument(String indexName, String id, SampleData data) throws IOException {
        data.setId(id);
        // Only update timestamp if it's not already set
        if (data.getTimestamp() == null) {
            data.setTimestamp(LocalDateTime.now());
        }
        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.id(id);
        
        String jsonData = objectMapper.writeValueAsString(data);
        indexRequest.source(jsonData, XContentType.JSON);
        
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return indexResponse.status() == RestStatus.OK || 
               indexResponse.getResult() == IndexResponse.Result.CREATED ||
               indexResponse.getResult() == IndexResponse.Result.UPDATED;
    }
    
    /**
     * Delete a document by ID
     * @param id document ID
     * @return true if successful, false otherwise
     * @throws IOException if communication with Elasticsearch fails
     */
    public boolean deleteDocument(String id) throws IOException {
        return deleteDocument(Constants.SAMPLE_INDEX_NAME, id);
    }
    
    /**
     * Delete a document by ID from the specified index
     * @param indexName index name
     * @param id document ID
     * @return true if successful, false otherwise
     * @throws IOException if communication with Elasticsearch fails
     */
    public boolean deleteDocument(String indexName, String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, id);
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        boolean success = deleteResponse.getResult() == DeleteResponse.Result.DELETED || 
               deleteResponse.status() == RestStatus.OK;
        log.info("Document deletion result for ID {}: {}", id, success);
        return success;
    }
    
    /**
     * Search documents by name
     * @param name name to search for
     * @return list of matching SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> searchDocumentsByName(String name) throws IOException {
        return searchDocumentsByName(Constants.SAMPLE_INDEX_NAME, name);
    }
    
    /**
     * Search documents by name in the specified index
     * @param indexName index name
     * @param name name to search for
     * @return list of matching SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> searchDocumentsByName(String indexName, String name) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(Constants.FIELD_NAME, name));
        searchRequest.source(searchSourceBuilder);
        
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return parseSearchResults(searchResponse);
    }
    
    /**
     * Search documents by wildcard pattern
     * @param field field to search in
     * @param pattern wildcard pattern (* and ? supported)
     * @return list of matching SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> searchDocumentsByWildcard(String field, String pattern) throws IOException {
        return searchDocumentsByWildcard(Constants.SAMPLE_INDEX_NAME, field, pattern);
    }
    
    /**
     * Search documents by wildcard pattern in the specified index
     * @param indexName index name
     * @param field field to search in
     * @param pattern wildcard pattern (* and ? supported)
     * @return list of matching SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> searchDocumentsByWildcard(String indexName, String field, String pattern) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        
        WildcardQueryBuilder wildcardQuery = QueryBuilders.wildcardQuery(field, pattern);
        searchSourceBuilder.query(wildcardQuery);
        searchRequest.source(searchSourceBuilder);
        
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return parseSearchResults(searchResponse);
    }
    
    /**
     * Search documents by date range
     * @param field field to search in
     * @param startDate start date (inclusive)
     * @param endDate end date (inclusive)
     * @return list of matching SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> searchDocumentsByDateRange(String field, LocalDateTime startDate, LocalDateTime endDate) throws IOException {
        return searchDocumentsByDateRange(Constants.SAMPLE_INDEX_NAME, field, startDate, endDate);
    }
    
    /**
     * Search documents by date range in the specified index
     * @param indexName index name
     * @param field field to search in
     * @param startDate start date (inclusive)
     * @param endDate end date (inclusive)
     * @return list of matching SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> searchDocumentsByDateRange(String indexName, String field, LocalDateTime startDate, LocalDateTime endDate) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        
        // Format dates to match the pattern in SampleData class
        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String startDateStr = startDate.format(formatter);
        String endDateStr = endDate.format(formatter);
        
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(field)
                .gte(startDateStr)
                .lte(endDateStr);
        
        searchSourceBuilder.query(rangeQuery);
        searchRequest.source(searchSourceBuilder);
        
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return parseSearchResults(searchResponse);
    }
    
    /**
     * Get document count in index
     * @return number of documents in the index
     * @throws IOException if communication with Elasticsearch fails
     */
    public long getDocumentCount() throws IOException {
        return getDocumentCount(Constants.SAMPLE_INDEX_NAME);
    }
    
    /**
     * Get document count in the specified index
     * @param indexName index name
     * @return number of documents in the index
     * @throws IOException if communication with Elasticsearch fails
     */
    public long getDocumentCount(String indexName) throws IOException {
        // Use search request with size 0 to get total hits
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(0); // We only need the count, not the documents
        searchRequest.source(searchSourceBuilder);
        
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse.getHits().getTotalHits().value;
    }
    
    /**
     * Search documents with sorting
     * @param sortField field to sort by
     * @param sortOrder sort order (ASC or DESC)
     * @param size number of results to return
     * @return list of matching SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> searchDocumentsWithSorting(String sortField, SortOrder sortOrder, int size) throws IOException {
        return searchDocumentsWithSorting(Constants.SAMPLE_INDEX_NAME, sortField, sortOrder, size);
    }
    
    /**
     * Search documents with sorting in the specified index
     * @param indexName index name
     * @param sortField field to sort by
     * @param sortOrder sort order (ASC or DESC)
     * @param size number of results to return
     * @return list of matching SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> searchDocumentsWithSorting(String indexName, String sortField, SortOrder sortOrder, int size) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.sort(sortField, sortOrder);
        searchSourceBuilder.size(size);
        searchRequest.source(searchSourceBuilder);
        
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return parseSearchResults(searchResponse);
    }
    
    /**
     * Get all documents
     * @return list of all SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> getAllDocuments() throws IOException {
        return getAllDocuments(Constants.SAMPLE_INDEX_NAME);
    }
    
    /**
     * Get all documents from the specified index
     * @param indexName index name
     * @return list of all SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> getAllDocuments(String indexName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return parseSearchResults(searchResponse);
    }
    
    /**
     * Parse search results into SampleData objects
     * @param searchResponse Elasticsearch search response
     * @return list of SampleData objects
     * @throws IOException if JSON parsing fails
     */
    private List<SampleData> parseSearchResults(SearchResponse searchResponse) throws IOException {
        List<SampleData> results = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            String jsonData = hit.getSourceAsString();
            SampleData data = objectMapper.readValue(jsonData, SampleData.class);
            data.setId(hit.getId());
            results.add(data);
        }
        return results;
    }
    
    /**
     * Delete the default sample index
     * @throws IOException if communication with Elasticsearch fails
     */
    public void deleteIndex() throws IOException {
        deleteIndex(Constants.SAMPLE_INDEX_NAME);
    }
    
    /**
     * Close the service and release resources
     */
    public void close() {
        ElasticsearchClientConfig.closeClient();
    }
}