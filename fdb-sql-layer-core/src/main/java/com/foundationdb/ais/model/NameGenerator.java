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

import java.util.List;
import java.util.Set;

/** NB: Used concurrently, synchronize implementations as appropriate. */
public interface NameGenerator
{
    // Generation
    int generateTableID(TableName name);
    int generateIndexID(int rootTableID);

    /** Generated named will be unique within the given {@code ais}. */
    TableName generateIdentitySequenceName(AkibanInformationSchema ais, TableName table, String column);

    String generateJoinName(TableName parentTable, TableName childTable, String[] pkColNames, String[] fkColNames);
    String generateJoinName(TableName parentTable, TableName childTable, List<JoinColumn> joinIndex);
    String generateJoinName(TableName parentTable, TableName childTable, List<String> pkColNames, List<String> fkColNames);
    String generateFullTextIndexPath(FullTextIndex index);
    TableName generateFKConstraintName(String schemaName, String tableName);
    TableName generatePKConstraintName(String schemaName, String tableName);
    TableName generateUniqueConstraintName(String schemaName, String tableName);
    
    // Bulk add
    void mergeAIS(AkibanInformationSchema ais);

    // Removal
    void removeTableID(int tableID);

    // View only (debug/testing)
    Set<String> getStorageNames();
}
