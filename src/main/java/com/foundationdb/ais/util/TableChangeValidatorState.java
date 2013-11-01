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

package com.foundationdb.ais.util;

import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.TableName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TableChangeValidatorState
{
    public static class TableColumnNames {
        public final TableName tableName;
        public final String oldColumnName;
        public final String newColumnName;

        public TableColumnNames(TableName tableName, String oldColumnName, String newColumnName) {
            this.tableName = tableName;
            this.oldColumnName = oldColumnName;
            this.newColumnName = newColumnName;
        }
    }

    public final List<TableChange> columnChanges;
    public final List<TableChange> tableIndexChanges;
    public final List<ChangedTableDescription> descriptions;
    public final Map<IndexName, List<TableColumnNames>> affectedGroupIndexes;


    public TableChangeValidatorState(List<TableChange> columnChanges,
                                     List<TableChange> tableIndexChanges) {
        this.columnChanges = columnChanges;
        this.tableIndexChanges = tableIndexChanges;
        this.descriptions = new ArrayList<>();
        this.affectedGroupIndexes = new TreeMap<>();
    }
}
