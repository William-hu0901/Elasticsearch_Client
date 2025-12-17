package org.daodao.elasticsearch.service;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test suite for ElasticsearchService tests
 * This suite includes all test classes for Elasticsearch functionality
 */
@Suite
@SelectClasses({
    ElasticsearchServiceTest.class,
    ElasticsearchServiceAdvancedTest.class
})
public class TestSuite {
    
    private static final Logger logger = LoggerFactory.getLogger(TestSuite.class);
    
    /**
     * Main method to run the test suite
     * This provides a convenient way to execute all tests together
     */
    public static void main(String[] args) {
        logger.info("Starting Elasticsearch test suite execution");
        
        // The actual test execution is handled by JUnit
        // This main method is just for documentation and explicit execution if needed
        
        logger.info("Elasticsearch test suite completed");
    }
}