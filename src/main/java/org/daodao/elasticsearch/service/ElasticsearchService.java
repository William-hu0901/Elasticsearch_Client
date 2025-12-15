package org.daodao.elasticsearch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.daodao.elasticsearch.Constants;
import org.daodao.elasticsearch.ElasticsearchClientConfig;
import org.daodao.elasticsearch.model.SampleData;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;


/**
 * Service class for Elasticsearch CRUD operations
 */
@Slf4j
public class ElasticsearchService {
    
    private final RestHighLevelClient client;
    private final ObjectMapper objectMapper;
    
    public ElasticsearchService() {
        this.client = ElasticsearchClientConfig.getClient();
        this.objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
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
     * Check if index exists
     * @param indexName index name
     * @return true if index exists, false otherwise
     * @throws IOException if communication with Elasticsearch fails
     */
    public boolean indexExists(String indexName) throws IOException {
        CountRequest countRequest = new CountRequest(indexName);
        countRequest.query(QueryBuilders.matchAllQuery());
        try {
            CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
            return true;
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
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
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
        IndexRequest indexRequest = new IndexRequest(Constants.SAMPLE_INDEX_NAME);
        
        if (data.getId() != null && !data.getId().isEmpty()) {
            indexRequest.id(data.getId());
        }
        
        String jsonData = objectMapper.writeValueAsString(data);
        indexRequest.source(jsonData, XContentType.JSON);
        
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        log.info("Document inserted with ID: {}", indexResponse.getId());
        return indexResponse.getId();
    }
    
    /**
     * Get a document by ID
     * @param id document ID
     * @return SampleData object or null if not found
     * @throws IOException if communication with Elasticsearch fails
     */
    public SampleData getDocument(String id) throws IOException {
        GetRequest getRequest = new GetRequest(Constants.SAMPLE_INDEX_NAME, id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        
        if (getResponse.isExists()) {
            String jsonData = getResponse.getSourceAsString();
            SampleData data = objectMapper.readValue(jsonData, SampleData.class);
            data.setId(getResponse.getId());
            return data;
        } else {
            log.warn("Document with ID {} not found", id);
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
        data.setId(id);
        data.setTimestamp(LocalDateTime.now());
        IndexRequest indexRequest = new IndexRequest(Constants.SAMPLE_INDEX_NAME);
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
        DeleteRequest deleteRequest = new DeleteRequest(Constants.SAMPLE_INDEX_NAME, id);
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        return deleteResponse.getResult() == DeleteResponse.Result.DELETED || 
               deleteResponse.status() == RestStatus.OK;
    }
    
    /**
     * Search documents by name
     * @param name name to search for
     * @return list of matching SampleData objects
     * @throws IOException if communication with Elasticsearch fails
     */
    public List<SampleData> searchDocumentsByName(String name) throws IOException {
        SearchRequest searchRequest = new SearchRequest(Constants.SAMPLE_INDEX_NAME);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(Constants.FIELD_NAME, name));
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
        SearchRequest searchRequest = new SearchRequest(Constants.SAMPLE_INDEX_NAME);
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