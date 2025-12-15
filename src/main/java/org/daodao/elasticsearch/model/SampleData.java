package org.daodao.elasticsearch.model;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Sample data model for Elasticsearch operations
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SampleData {
    private String id;
    private String name;
    private String description;
    private LocalDateTime timestamp;
    
    public SampleData(String name, String description) {
        this.name = name;
        this.description = description;
        this.timestamp = LocalDateTime.now();
    }
}