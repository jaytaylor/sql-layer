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

package com.akiban.server.util;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;

public class GroupIndexCreator {
    public static class GroupIndexCreatorException extends Exception {
        public GroupIndexCreatorException(String msg) {
            super(msg);
        }
    }

    /**
     * Helper function for converting a simple group index specification into an
     * actual GroupIndex. This can then be passed to DDLFunctions.createGroupIndex().
     *
     * @param ais AIS that contains referenced group and tables.
     * @param groupName Name of the group to add index too
     * @param indexName Name of the new index to create
     * @param tableColumnList Comma separated list of tableName.columnName pairs.
     * @return GroupIndex representation of the requested
     * @throws GroupIndexCreatorException For any error
     */
    public static GroupIndex createIndex(AkibanInformationSchema ais, String groupName, String indexName,
                                         String tableColumnList) throws GroupIndexCreatorException {
        final Group group = ais.getGroup(groupName);
        if(group == null) {
            throw new GroupIndexCreatorException("Unknown group: " + groupName);
        }
        if(indexName.isEmpty()) {
            throw new GroupIndexCreatorException("No index name specified");
        }

        final String tableColPairs[] = tableColumnList.split(",");

        int pos = 0;
        final GroupIndex tmpIndex = new GroupIndex(group, indexName, 0, false, "KEY");
        for(String tableCol : tableColPairs) {
            int period = tableCol.indexOf('.');
            if(period == -1) {
                throw new GroupIndexCreatorException("Expected table.column: " + tableCol);
            }
            final String tableName = tableCol.substring(0, period).trim();
            final String columnName = tableCol.substring(period+1).trim();
            final UserTable table = findTableInGroup(ais, group, tableName);
            if(table == null) {
                throw new GroupIndexCreatorException("Table does not exist in group: " + tableName);
            }
            final Column column = table.getColumn(columnName);
            if(column == null) {
                throw new GroupIndexCreatorException("Table does not contain column: " + columnName);
            }
            tmpIndex.addColumn(new IndexColumn(tmpIndex, column, pos++, true, null));
        }

        return tmpIndex;
    }

    private static UserTable findTableInGroup(AkibanInformationSchema ais, Group group, String tableName) {
        for(UserTable table : ais.getUserTables().values()) {
            if(table.getGroup().equals(group) && table.getName().getTableName().equals(tableName)) {
                return table;
            }
        }
        return null;
    }
}
