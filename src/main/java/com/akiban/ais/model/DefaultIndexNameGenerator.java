
package com.akiban.ais.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DefaultIndexNameGenerator implements IndexNameGenerator {
    private final Set<String> indexNames = new HashSet<>();

    public DefaultIndexNameGenerator(Collection<String> initialIndexNames) {
        indexNames.addAll(initialIndexNames);
    }

    public static DefaultIndexNameGenerator forTable(UserTable table) {
        Set<String> indexNames = new HashSet<>();
        for(Index index : table.getIndexesIncludingInternal()) {
            indexNames.add(index.getIndexName().getName());
        }
        return new DefaultIndexNameGenerator(indexNames);
    }

    @Override
    public String generateIndexName(String indexName, String columnName, String constraint) {
        if((indexName != null) && !indexNames.contains(indexName)) {
            indexNames.add(indexName);
            return indexName;
        }
        String name = columnName;
        for(int suffixNum = 2; indexNames.contains(name); ++suffixNum) {
            name = String.format("%s_%d", columnName, suffixNum);
        }
        indexNames.add(name);
        return name;
    }
}
