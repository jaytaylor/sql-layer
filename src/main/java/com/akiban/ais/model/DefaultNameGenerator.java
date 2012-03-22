/**
 * Copyright (C) 2011 Akiban Technologies Inc.
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
package com.akiban.ais.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.akiban.ais.model.AISBuilder.ColumnName;
import com.akiban.util.Strings;

public class DefaultNameGenerator implements NameGenerator {

    /**
     * For truncated columns [only], we record a mapping of the original
     * column to truncated name. This lets us ensure that unique columns
     * have unique truncated names. We use HashMap instead of Map to make
     * life easier for GWT.
     */
    private final HashMap<ColumnName, String> generatedColumnNames = new HashMap<ColumnName, String>();
    private final Set<String> groupNames = new HashSet<String>();
    private final Set<String> indexNames = new HashSet<String>();
    
    @Override
    public String generateColumnName(Column column) {
        UserTable table = (UserTable) column.getTable();

        StringBuilder ret = new StringBuilder(table.getName()
                .getTableName()).append("$").append(column.getName());

        if (ret.length() <= AISBuilder.MAX_COLUMN_NAME_LENGTH) {
            return ret.toString();
        }
        final ColumnName id = new ColumnName(table.getName(),
                column.getName());
        {
            String possible = generatedColumnNames.get(id);
            if (possible != null) {
                return possible;
            }
        }

        // We need to truncate, but first see if this column name is already
        // known
        ret.delete(0, ret.length() - AISBuilder.MAX_COLUMN_NAME_LENGTH);
        int anonId = 0;
        String retValue;
        while (generatedColumnNames.containsValue((retValue = ret
                .toString()))) {
            int digits = countDigits(++anonId);
            int len = ret.length();
            ret.delete(len - (digits + 1), len);
            ret.append('$').append(anonId);
        }

        generatedColumnNames.put(id, retValue);

        return retValue;
    }
    
    /**
     * Counts the number of digits in the int
     * 
     * @param number
     *            {@code >= 0}
     * @return number of digits
     */
    private int countDigits(int number) {
        int ret = 1;
        while ((number /= 10) > 0) {
            ++ret;
        }
        return ret;
    }

    @Override
    public String generateGroupIndexName(TableIndex userTableIndex) {
        return userTableIndex.getTable().getName().getTableName() + "$"
        + userTableIndex.getIndexName().getName();
    }

    @Override
    public String generateGroupName(UserTable userTable) {
        return generateGroupName(userTable.getName().getTableName());
    }
    
    @Override
    public String generateGroupName(final String tableName) {
        String startingName = tableName;
        if (groupNames.add(startingName)) {
            return startingName;
        }
        int i = 0;
        StringBuilder builder = new StringBuilder(startingName).append('$');
        final int appendAt = builder.length();
        String ret;

        do {
            builder.setLength(appendAt);
            builder.append(i++);
        }
        while(!groupNames.add(ret = builder.toString()));
        
        return ret;
    }

    @Override
    public String generateGroupTableName (final String groupName) {
        return "_akiban_" + groupName;
    }

    public DefaultNameGenerator setDefaultGroupNames (Set<String> initialSet) {
        groupNames.addAll(initialSet);
        return this;
    }
    
    @Override
    public String generateIndexName(String indexName, String columnName,
            String constraint) {
        if (constraint.equals(Index.PRIMARY_KEY_CONSTRAINT)) {
            indexNames.add(Index.PRIMARY_KEY_CONSTRAINT);
            return Index.PRIMARY_KEY_CONSTRAINT;
        }
        
        if (indexName != null && !indexNames.contains(indexName)) {
            indexNames.add(indexName);
            return indexName;
        }
        
        String name = columnName;
        for (int suffixNum=2; indexNames.contains(name); ++suffixNum) {
            name = String.format("%s_%d", columnName, suffixNum);
        }
        indexNames.add(name);
        return name;
    }
    
    @Override
    public String generateJoinName (TableName parentTable, TableName childTable, List<JoinColumn> columns) {
        List<String> pkColNames = new LinkedList<String>();
        List<String> fkColNames = new LinkedList<String>();
        for (JoinColumn col : columns) {
            pkColNames.add(col.getParent().getName());
            fkColNames.add(col.getChild().getName());
        }
        String ret = String.format("%s/%s/%s/%s/%s/%s",
                parentTable.getSchemaName(),
                parentTable.getTableName(),
                Strings.join(pkColNames, ","),
                childTable.getSchemaName(),
                childTable, // TODO: This shold be getTableName(), but preserve old behavior for test existing output
                Strings.join(fkColNames, ","));
        return ret.toLowerCase().replace(',', '_');
    }
}
