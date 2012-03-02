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

package com.akiban.server.service.dxl;

import com.akiban.ais.model.Index;

import java.util.List;

@SuppressWarnings("unused") // jmx
public interface DXLMXBean {
    String getUsingSchema();

    void recreateGroupIndexes();

    void setUsingSchema(String schema);

    void createGroupIndex(String groupName, String indexName, String tableColumnList, Index.JoinType joinType);

    void dropTable(String tableName) ;

    void dropGroupIndex(String groupName, String indexName) ;

    void dropGroup(String groupName) ;
    
    void dropGroupBySchema(String schemaName) ;

    void dropAllGroups() ;

    void writeRow(String table, String fields) ;

    List<String> getGrouping();

    List<String> getGroupIndexDDLs();

    String getGroupNameFromTableName(String schema, String table);

    String printAIS();
}
