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
import com.akiban.ais.model.CharsetAndCollation;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.Type;

import java.util.HashMap;
import java.util.Map;

public class MMColumn implements ModelNames {
    public static Column create(AkibanInformationSchema ais, Map<String, Object> map)
    {

        Column column = null;
        String schemaName = (String) map.get(column_schemaName);
        String tableName = (String) map.get(column_tableName);
        Table table = ais.getTable(schemaName, tableName);
        if (table != null) {
            Type type = ais.getType((String) map.get(column_typename));
            CharsetAndCollation charAndCol = CharsetAndCollation.intern((String) map.get(column_charset),
                                                                        (String) map.get(column_collation));
            Long param1 = null;
            Long param2 = null;
            Integer nParameters = type.nTypeParameters();
            if (nParameters >= 1) {
                param1 = (Long) map.get(column_typeParam1);
                if (nParameters >= 2) {
                    param2 = (Long) map.get(column_typeParam2);
                }
            }
            
            return Column.create(table,
                                 (String) map.get(column_columnName),
                                 (Integer) map.get(column_position),
                                 type,
                                 (Boolean) map.get(column_nullable),
                                 param1,
                                 param2,
                                 (Long) map.get(column_initialAutoIncrementValue),
                                 charAndCol);
        }
        return column;
    }


    public static Map<String, Object> map(Column column)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        String groupSchemaName = null;
        String groupTableName = null;
        String groupColumnName = null;
        Column groupColumn = column.getGroupColumn();
        if (groupColumn != null) {
            groupSchemaName = groupColumn.getTable().getName().getSchemaName();
            groupTableName = groupColumn.getTable().getName().getTableName();
            groupColumnName = groupColumn.getName();
        }
        Table table = column.getTable();
        map.put(column_schemaName, table.getName().getSchemaName());
        map.put(column_tableName, table.getName().getTableName());
        map.put(column_columnName, column.getName());
        map.put(column_position, column.getPosition());
        Type type = column.getType();
        map.put(column_typename, type.name());
        map.put(column_typeParam1, type.nTypeParameters() >= 1 ? column.getTypeParameter1() : null);
        map.put(column_typeParam2, type.nTypeParameters() >= 2 ? column.getTypeParameter2() : null);
        map.put(column_nullable, column.getNullable());
        map.put(column_maxStorageSize, column.getMaxStorageSize());
        map.put(column_prefixSize, column.getPrefixSize());
        map.put(column_initialAutoIncrementValue, column.getInitialAutoIncrementValue());
        map.put(column_groupSchemaName, groupSchemaName);
        map.put(column_groupTableName, groupTableName);
        map.put(column_groupColumnName, groupColumnName);
        CharsetAndCollation charsetAndCollation = column.getCharsetAndCollation();
        map.put(column_charset, charsetAndCollation.charset());
        map.put(column_collation, charsetAndCollation.collation());
        return map;
    }
}
