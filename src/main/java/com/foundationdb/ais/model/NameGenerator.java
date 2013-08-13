/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

public interface NameGenerator
{
    // Generation
    int generateTableID(TableName name);
    int generateIndexID(int rootTableID);
    TableName generateIdentitySequenceName(TableName table);
    String generateJoinName(TableName parentTable, TableName childTable, List<JoinColumn> joinIndex);
    String generateJoinName(TableName parentTable, TableName childTable, List<String> pkColNames, List<String> fkColNames);
    String generateIndexTreeName(Index index);
    String generateGroupTreeName(String schemaName, String groupName);
    String generateSequenceTreeName(Sequence sequence);

    // Removal
    void removeTableID(int tableID);
    void removeTreeName(String treeName);

    // View only (debug/testing)
    Set<String> getTreeNames();
}
