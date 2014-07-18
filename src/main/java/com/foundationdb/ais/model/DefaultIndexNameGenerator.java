/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.ais.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DefaultIndexNameGenerator implements IndexNameGenerator {
    private final Set<String> indexNames = new HashSet<>();

    public DefaultIndexNameGenerator(Collection<String> initialIndexNames) {
        indexNames.addAll(initialIndexNames);
    }

    public static DefaultIndexNameGenerator forTable(Table table) {
        Set<String> indexNames = new HashSet<>();
        for(Index index : table.getIndexesIncludingInternal()) {
            indexNames.add(index.getIndexName().getName());
        }
        return new DefaultIndexNameGenerator(indexNames);
    }

    @Override
    public String generateIndexName(String indexName, String columnName) {
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
