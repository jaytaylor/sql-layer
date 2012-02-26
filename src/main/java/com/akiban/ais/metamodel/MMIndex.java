/**
 * Copyright (C) 2012 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.metamodel;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;

import java.util.HashMap;
import java.util.Map;

public class MMIndex implements ModelNames {

    public static Index create(AkibanInformationSchema ais, Map<String, Object> map)
    {
        Index index = null;
        String schemaName = (String) map.get(index_schemaName);
        String tableName = (String) map.get(index_tableName);
        String indexType = (String) map.get(index_indexType);
        String indexName = (String) map.get(index_indexName);
        Integer indexId = (Integer) map.get(index_indexId);
        Boolean unique = (Boolean) map.get(index_unique);
        String constraint = (String) map.get(index_constraint);
        if(Index.IndexType.TABLE.toString().equals(indexType)) {
            Table table = ais.getTable(schemaName, tableName);
            if (table != null) {
                index = TableIndex.create(ais, table, indexName, indexId, unique, constraint);
            }
        }
        else if(Index.IndexType.GROUP.toString().equals(indexType)) {
            Group group = ais.getGroup(tableName);
            if (group != null) {
                index = GroupIndex.create(ais, group, indexName, indexId, unique, constraint);
            }
        }
        if(index != null) {
            index.setTreeName((String) map.get(index_treeName));
        }
        return index;
    }

    public static Map<String, Object> map(Index index)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        IndexName indexName = index.getIndexName();
        map.put(index_schemaName, indexName.getSchemaName());
        map.put(index_tableName, indexName.getTableName());
        map.put(index_indexType, index.getIndexType().toString());
        map.put(index_indexName, indexName.getName());
        map.put(index_indexId, index.getIdAndFlags());
        map.put(index_unique, index.isUnique());
        map.put(index_constraint, index.getConstraint());
        map.put(index_treeName, index.getTreeName());
        return map;
    }
}
