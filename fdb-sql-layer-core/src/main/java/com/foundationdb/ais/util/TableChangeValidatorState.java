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

import com.foundationdb.ais.model.ColumnName;
import com.foundationdb.ais.model.TableName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TableChangeValidatorState
{
    public final List<TableChange> columnChanges;
    public final List<TableChange> tableIndexChanges;
    public final List<String> droppedGI;
    /** Any GI part of the group that is changing and still present after. */
    public final Map<String, List<ColumnName>> affectedGI;
    /** GI with data change, subset of {@link #affectedGI}. */
    public final Map<String, List<ColumnName>> dataAffectedGI;
    public final List<ChangedTableDescription> descriptions;


    public TableChangeValidatorState(List<TableChange> columnChanges,
                                     List<TableChange> tableIndexChanges) {
        this.columnChanges = columnChanges;
        this.tableIndexChanges = tableIndexChanges;
        this.droppedGI = new ArrayList<>();
        this.affectedGI = new TreeMap<>();
        this.dataAffectedGI = new TreeMap<>();
        this.descriptions = new ArrayList<>();
    }

    public boolean hasOldTable(TableName name) {
        for(ChangedTableDescription desc : descriptions) {
            if(desc.getOldName().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
