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

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.util.ArgumentValidation;

import java.util.Collection;
import java.util.Map;

/**
 * Information describing the state of an altered table
 */
public class ChangedTableDescription {
    public static enum ParentChange {
        /** No change at all **/
        NONE,
        /** Metadata only change (e.g. rename o.cid) **/
        META,
        /** Group is changing but not the relationship (e.g. i is UPDATE when c.id changes type) **/
        UPDATE,
        /** New parent **/
        ADD,
        /** Dropped parent **/
        DROP
    }

    private final int tableID;
    private final TableName tableName;
    private final Table newDefinition;
    private final Map<String,String> colNames;
    private final ParentChange parentChange;
    private final TableName parentName;
    private final Map<String,String> parentColNames;
    private final Map<String,String> preserveIndexes;
    private final Collection<TableName> droppedSequences;
    private final Collection<String> identityAdded;
    private final Collection<String> indexesAdded;
    private final boolean isTableAffected;
    private final boolean isPKAffected;

    /**
     * @param tableName Current name of the table being changed.
     * @param newDefinition New definition of the table.
     * @param preserveIndexes Mapping of new index names to old.
     */
    public ChangedTableDescription(int tableID, TableName tableName, Table newDefinition, Map<String,String> colNames,
                                   ParentChange parentChange, TableName parentName, Map<String,String> parentColNames,
                                   Map<String, String> preserveIndexes, Collection<TableName> droppedSequences,
                                   Collection<String> identityAdded, Collection<String> indexesAdded,
                                   boolean isTableAffected, boolean isPKAffected) {
        ArgumentValidation.notNull("tableName", tableName);
        ArgumentValidation.notNull("preserveIndexes", preserveIndexes);
        this.tableID = tableID;
        this.tableName = tableName;
        this.newDefinition = newDefinition;
        this.colNames = colNames;
        this.parentChange = parentChange;
        this.parentName = parentName;
        this.parentColNames = parentColNames;
        this.preserveIndexes = preserveIndexes;
        this.droppedSequences = droppedSequences;
        this.identityAdded = identityAdded;
        this.indexesAdded = indexesAdded;
        this.isTableAffected = isTableAffected;
        this.isPKAffected = isPKAffected;
    }

    public int getTableID() {
        return tableID;
    }

    public TableName getOldName() {
        return tableName;
    }

    public TableName getNewName() {
        return (newDefinition != null) ? newDefinition.getName() : tableName;
    }

    public Table getNewDefinition() {
        return newDefinition;
    }

    public Map<String,String> getColNames() {
        return colNames;
    }

    public ParentChange getParentChange() {
        return parentChange;
    }

    public TableName getParentName() {
        return parentName;
    }

    public Map<String,String> getParentColNames() {
        return parentColNames;
    }

    public Map<String,String> getPreserveIndexes() {
        return preserveIndexes;
    }

    public Collection<TableName> getDroppedSequences() {
        return droppedSequences;
    }

    public Collection<String> getIdentityAdded() {
        return identityAdded;
    }

    public Collection<String> getIndexesAdded() {
        return indexesAdded;
    }

    public boolean isTableAffected() {
        return isTableAffected;
    }

    public boolean isPKAffected() {
        return isPKAffected;
    }

    public boolean isNewGroup() {
        return (parentChange != ParentChange.NONE) && (parentChange != ParentChange.META);
    }

    @Override
    public String toString() {
        return toString(getOldName(), getNewName(), isNewGroup(), getParentChange(), getPreserveIndexes());
    }

    public static String toString(TableName oldName, TableName newName, boolean newGroup, ParentChange groupChange, Map<String,String> preservedIndexMap) {
        return oldName + "=" + newName + "[newGroup=" + newGroup + "][parentChange=" + groupChange + "]" + preservedIndexMap.toString();
    }
}
