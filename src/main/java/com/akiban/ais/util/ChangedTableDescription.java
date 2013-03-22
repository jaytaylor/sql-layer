
package com.akiban.ais.util;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.util.ArgumentValidation;

import java.util.Collection;
import java.util.Map;

/**
 * Information describing the state of an altered table
 */
public class ChangedTableDescription {
    public static enum ParentChange { NONE, UPDATE, ADD, DROP }

    private final TableName tableName;
    private final UserTable newDefinition;
    private final Map<String,String> colNames;
    private final ParentChange parentChange;
    private final TableName parentName;
    private final Map<String,String> parentColNames;
    private final Map<String,String> preserveIndexes;
    private final Collection<TableName> droppedSequences;

    /**
     * @param tableName Current name of the table being changed.
     * @param newDefinition New definition of the table.
     * @param preserveIndexes Mapping of new index names to old.
     */
    public ChangedTableDescription(TableName tableName, UserTable newDefinition, Map<String,String> colNames,
                                   ParentChange parentChange, TableName parentName, Map<String,String> parentColNames,
                                   Map<String, String> preserveIndexes, Collection<TableName> droppedSequences) {
        ArgumentValidation.notNull("tableName", tableName);
        ArgumentValidation.notNull("preserveIndexes", preserveIndexes);
        this.tableName = tableName;
        this.newDefinition = newDefinition;
        this.colNames = colNames;
        this.parentChange = parentChange;
        this.parentName = parentName;
        this.parentColNames = parentColNames;
        this.preserveIndexes = preserveIndexes;
        this.droppedSequences = droppedSequences;
    }

    public TableName getOldName() {
        return tableName;
    }

    public TableName getNewName() {
        return (newDefinition != null) ? newDefinition.getName() : tableName;
    }

    public UserTable getNewDefinition() {
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

    public boolean isNewGroup() {
        return (parentChange != ParentChange.NONE);
    }

    @Override
    public String toString() {
        return toString(getOldName(), getNewName(), isNewGroup(), getParentChange(), getPreserveIndexes());
    }

    public static String toString(TableName oldName, TableName newName, boolean newGroup, ParentChange groupChange, Map<String,String> indexMap) {
        return oldName + "=" + newName + "[newGroup=" + newGroup + "][parentChange=" + groupChange + "]" + indexMap.toString();
    }
}
