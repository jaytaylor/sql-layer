/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.metamodel;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;

import java.util.HashMap;
import java.util.Map;

public class MMIndexColumn implements ModelNames {
    public static IndexColumn create(AkibanInformationSchema ais, Map<String, Object> map)
    {
        IndexColumn indexColumn = null;
        String schemaName = (String) map.get(indexColumn_schemaName);
        String tableName = (String) map.get(indexColumn_tableName);
        String indexType = (String) map.get(indexColumn_indexType);
        String indexName = (String) map.get(indexColumn_indexName);
        String columnName = (String) map.get(indexColumn_columnName);
        Integer position = (Integer) map.get(indexColumn_position);
        Boolean ascending = (Boolean) map.get(indexColumn_ascending);
        Integer indexedLength = (Integer) map.get(indexColumn_indexedLength);
        Table table = ais.getTable(schemaName, tableName);
        Index index = null;
        if(table != null) {
            if(Index.IndexType.GROUP.toString().endsWith(indexType)) {
                Group group = table.getGroup();
                if (group != null) {
                    index = group.getIndex(indexName);
                }
            }
            else {
                index = table.getIndex(indexName);
            }
            if (index != null) {
                Column column = table.getColumn(columnName.toLowerCase());
                if (column != null) {
                    indexColumn = IndexColumn.create(index, column, position, ascending, indexedLength);
                }
            }
        }
        return indexColumn;
    }

    public static Map<String, Object> map(IndexColumn indexColumn)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        Column column = indexColumn.getColumn();
        map.put(indexColumn_schemaName, column.getTable().getName().getSchemaName());
        map.put(indexColumn_tableName, column.getTable().getName().getTableName());
        Index index = indexColumn.getIndex();
        map.put(indexColumn_indexType, index.getIndexType().toString());
        map.put(indexColumn_indexName, index.getIndexName().getName());
        map.put(indexColumn_columnName, column.getName());
        map.put(indexColumn_position, indexColumn.getPosition());
        map.put(indexColumn_ascending, indexColumn.isAscending());
        map.put(indexColumn_indexedLength, indexColumn.getIndexedLength());
        return map;
    }
}
 