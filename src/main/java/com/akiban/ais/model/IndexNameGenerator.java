
package com.akiban.ais.model;

public interface IndexNameGenerator {
    String generateIndexName(String indexName, String columnName, String constraint);
}
