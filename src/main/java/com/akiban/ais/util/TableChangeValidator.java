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

package com.akiban.ais.util;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.types3.Types3Switch;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.MultipleCauseException;
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.akiban.ais.util.ChangedTableDescription.ParentChange;
import static com.akiban.ais.util.TableChangeValidatorException.*;

public class TableChangeValidator {
    private static final Map<String,String> EMPTY_STRING_MAP = Collections.emptyMap();
    private static final List<TableName> EMPTY_TABLE_NAME_LIST = Collections.emptyList();

    public static enum ChangeLevel {
        NONE,
        METADATA,
        METADATA_NOT_NULL,
        INDEX,
        TABLE,
        GROUP;

        public boolean isNoneOrMetaData() {
            return (this == NONE) || (this == METADATA) || (this == METADATA_NOT_NULL);
        }
    }

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

    private final UserTable oldTable;
    private final UserTable newTable;
    private final List<TableChange> columnChanges;
    private final List<TableChange> indexChanges;
    private final List<RuntimeException> errors;
    private final List<RuntimeException> unmodifiedChanges;
    private final Collection<ChangedTableDescription> changedTables;
    private final Map<IndexName, List<TableColumnNames>> affectedGroupIndexes;
    private final boolean automaticIndexChanges;
    private ChangeLevel finalChangeLevel;
    private ChangedTableDescription.ParentChange parentChange;
    private boolean primaryKeyChanged;
    private boolean didCompare;

    public TableChangeValidator(UserTable oldTable, UserTable newTable,
                                List<TableChange> columnChanges, List<TableChange> indexChanges,
                                boolean automaticIndexChanges) {
        ArgumentValidation.notNull("oldTable", oldTable);
        ArgumentValidation.notNull("newTable", newTable);
        ArgumentValidation.notNull("columnChanges", columnChanges);
        ArgumentValidation.notNull("indexChanges", indexChanges);
        this.oldTable = oldTable;
        this.newTable = newTable;
        this.columnChanges = columnChanges;
        this.indexChanges = indexChanges;
        this.unmodifiedChanges = new ArrayList<>();
        this.errors = new ArrayList<>();
        this.changedTables = new ArrayList<>();
        this.affectedGroupIndexes = new TreeMap<>();
        this.automaticIndexChanges = automaticIndexChanges;
        this.finalChangeLevel = ChangeLevel.NONE;
        this.parentChange = ParentChange.NONE;
    }

    public ChangeLevel getFinalChangeLevel() {
        return finalChangeLevel;
    }

    public Collection<ChangedTableDescription> getAllChangedTables() {
        return changedTables;
    }

    public Map<IndexName, List<TableColumnNames>> getAffectedGroupIndexes() {
        return affectedGroupIndexes;
    }

    public boolean isParentChanged() {
        return (parentChange == ParentChange.ADD) || (parentChange == ParentChange.DROP);
    }

    public boolean isPrimaryKeyChanged() {
        return primaryKeyChanged;
    }

    public List<RuntimeException> getUnmodifiedChanges() {
        return unmodifiedChanges;
    }

    public void compare() {
        if(!didCompare) {
            compareTable();
            compareColumns();
            compareIndexes(automaticIndexChanges);
            compareGrouping();
            compareGroupIndexes();
            updateFinalChangeLevel(ChangeLevel.NONE);
            didCompare = true;
        }
    }

    public void compareAndThrowIfNecessary() {
        compare();
        switch(errors.size()) {
            case 0:
                return;
            case 1:
                throw errors.get(0);
            default:
                MultipleCauseException mce = new MultipleCauseException();
                for(Exception e : errors) {
                    mce.addCause(e);
                }
                throw mce;
        }
    }

    private void updateFinalChangeLevel(ChangeLevel level) {
        if(errors.isEmpty()) {
            if(level.ordinal() > finalChangeLevel.ordinal()) {
                finalChangeLevel = level;
            }
        } else {
            finalChangeLevel = null;
        }
    }

    private void compareTable() {
        TableName oldName = oldTable.getName();
        TableName newName = newTable.getName();
        if(!oldName.equals(newName) ||
           !Objects.equal(oldTable.getCharsetAndCollation(), newTable.getCharsetAndCollation())) {
            updateFinalChangeLevel(ChangeLevel.METADATA);
        }
    }

    private void compareColumns() {
        Map<String,Column> oldColumns = new HashMap<>();
        Map<String,Column> newColumns = new HashMap<>();
        for(Column column : oldTable.getColumns()) {
            oldColumns.put(column.getName(), column);
        }
        for(Column column : newTable.getColumns()) {
            newColumns.put(column.getName(), column);
        }
        checkChanges(ChangeLevel.TABLE, columnChanges, oldColumns, newColumns, false);

        // Look for position changes, not required to be declared
        for(Map.Entry<String, Column> oldEntry : oldColumns.entrySet()) {
            Column newColumn = newColumns.get(findNewName(columnChanges, oldEntry.getKey()));
            if((newColumn != null) && !oldEntry.getValue().getPosition().equals(newColumn.getPosition())) {
                updateFinalChangeLevel(ChangeLevel.TABLE);
                break;
            }
        }
    }

    private void compareIndexes(boolean autoChanges) {
        Map<String,TableIndex> oldIndexes = new HashMap<>();
        Map<String,TableIndex> newIndexes = new HashMap<>();
        for(TableIndex index : oldTable.getIndexes()) {
            oldIndexes.put(index.getIndexName().getName(), index);
        }

        if(autoChanges) {
            // Look for incompatible spatial changes
            for(TableIndex oldIndex : oldTable.getIndexes()) {
                String newName = findNewName(indexChanges, oldIndex.getIndexName().getName());
                TableIndex newIndex = (newName != null) ? (TableIndex)newTable.getIndexIncludingInternal(newName) : null;
                if((newIndex != null) && oldIndex.isSpatial() && !Index.isSpatialCompatible(newIndex)) {
                    newTable.removeIndexes(Collections.singleton(newIndex));
                }
            }
        }

        for(TableIndex index : newTable.getIndexes()) {
            newIndexes.put(index.getIndexName().getName(), index);
        }

        checkChanges(ChangeLevel.INDEX, indexChanges, oldIndexes, newIndexes, autoChanges);
    }

    private void compareGroupIndexes() {
        final Set<UserTable> keepTables = new HashSet<>();
        final UserTable traverseStart;
        if(parentChange == ParentChange.DROP) {
            traverseStart = oldTable;
        } else {
           traverseStart = oldTable.getGroup().getRoot();
        }

        traverseStart.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable table) {
                keepTables.add(table);
            }
        });

        for(GroupIndex index : oldTable.getGroupIndexes()) {
            boolean hadChange = (finalChangeLevel == ChangeLevel.GROUP);
            List<TableColumnNames> remainingCols = new ArrayList<>();
            for(IndexColumn iCol : index.getKeyColumns()) {
                Column column = iCol.getColumn();
                if(!keepTables.contains(column.getUserTable())) {
                    remainingCols.clear();
                    break;
                }
                String oldName = column.getName();
                String newName = (column.getTable() != oldTable) ? oldName : findNewName(columnChanges, oldName);
                if(newName != null) {
                    remainingCols.add(new TableColumnNames(column.getTable().getName(), oldName, newName));
                } else {
                    hadChange = true;
                }
            }
            if(remainingCols.size() <= 1) {
                remainingCols.clear();
                affectedGroupIndexes.put(index.getIndexName(), remainingCols);
            } else {
                // Check if any from this table were changed, not affected if not
                for(TableColumnNames tcn : remainingCols) {
                    if(hadChange) {
                        break;
                    }
                    if(tcn.tableName.equals(oldTable.getName())) {
                        Column oldColumn = oldTable.getColumn(tcn.oldColumnName);
                        Column newColumn = newTable.getColumn(tcn.newColumnName);
                        hadChange = !tcn.oldColumnName.equals(tcn.newColumnName) ||
                                    (compare(oldColumn, newColumn) == ChangeLevel.TABLE);
                    }
                }
                if(hadChange) {
                    affectedGroupIndexes.put(index.getIndexName(), remainingCols);
                }
            }
        }
    }

    private <T> void checkChanges(ChangeLevel level, List<TableChange> changeList, Map<String,T> oldMap, Map<String,T> newMap, boolean doAutoChanges) {
        final boolean isIndex = (level == ChangeLevel.INDEX);
        Set<String> oldExcludes = new HashSet<>();
        Set<String> newExcludes = new HashSet<>();

        List<TableChange> autoChanges = doAutoChanges ? new ArrayList<TableChange>() : null;

        // Check declared changes
        for(TableChange change : changeList) {
            String oldName = change.getOldName();
            String newName = change.getNewName();
            switch(change.getChangeType()) {
                case ADD: {
                    if(newMap.get(newName) == null) {
                        if(doAutoChanges) {
                            autoChanges.add(TableChange.createAdd(newName));
                        } else {
                            addNotPresent(isIndex, change);
                        }
                    } else {
                        updateFinalChangeLevel(level);
                        newExcludes.add(newName);
                    }
                }
                break;

                case DROP: {
                    if(oldMap.get(oldName) == null) {
                        if(doAutoChanges) {
                            autoChanges.add(TableChange.createDrop(oldName));
                        } else {
                            dropNotPresent(isIndex, change);
                        }
                    } else {
                        updateFinalChangeLevel(level);
                        oldExcludes.add(oldName);
                    }
                }
                break;

                case MODIFY: {
                    T oldVal = oldMap.get(oldName);
                    T newVal = newMap.get(newName);
                    if((oldVal == null) || (newVal == null)) {
                        if(!doAutoChanges) {
                            modifyNotPresent(isIndex, change);
                        }
                    } else {
                        ChangeLevel curChange = compare(oldVal, newVal);
                        if(curChange == ChangeLevel.NONE) {
                            unmodifiedChanges.add(modifyNotChanged(isIndex, change));
                        } else {
                            updateFinalChangeLevel(curChange);
                            oldExcludes.add(oldName);
                            newExcludes.add(newName);
                        }
                    }
                }
                break;
            }
        }

        // Check remaining elements in old table
        for(Map.Entry<String,T> entry : oldMap.entrySet()) {
            String name = entry.getKey();
            if(!oldExcludes.contains(name)) {
                T newVal = newMap.get(name);
                if(newVal == null) {
                    if(doAutoChanges) {
                        autoChanges.add(TableChange.createDrop(name));
                    } else {
                        unchangedNotPresent(isIndex, name);
                    }
                } else {
                    ChangeLevel change = compare(entry.getValue(), newVal);
                    if(change != ChangeLevel.NONE) {
                        if(doAutoChanges) {
                            autoChanges.add(TableChange.createModify(name, name));
                        } else {
                            undeclaredChange(isIndex, name);
                        }
                    }
                    newExcludes.add(name);
                }
            }
        }

        // Check remaining elements in new table (should be none)
        for(String name : newMap.keySet()) {
            if(!newExcludes.contains(name)) {
                if(doAutoChanges) {
                    autoChanges.add(TableChange.createDrop(name));
                } else {
                    undeclaredChange(isIndex, name);
                }
            }
        }

        if(doAutoChanges && !autoChanges.isEmpty()) {
            changeList.addAll(autoChanges);
            updateFinalChangeLevel(level);
        }
    }

    private void compareGrouping() {
        parentChange = compareParentJoin(columnChanges, oldTable.getParentJoin(), newTable.getParentJoin());
        primaryKeyChanged = containsOldOrNew(indexChanges, Index.PRIMARY_KEY_CONSTRAINT);

        List<TableName> droppedSequences = new ArrayList<>();
        List<String> addedIdentity = new ArrayList<>();
        Map<String,String> renamedColumns = new HashMap<>();
        for(TableChange change : columnChanges) {
            switch(change.getChangeType()) {
                case MODIFY: {
                    if(!change.getOldName().equals(change.getNewName())) {
                        renamedColumns.put(change.getOldName(), change.getNewName());
                    }
                    Column oldColumn = oldTable.getColumn(change.getOldName());
                    Column newColumn = newTable.getColumn(change.getNewName());
                    if((oldColumn != null)) {
                        Sequence oldSeq = oldColumn.getIdentityGenerator();
                        Sequence newSeq = newColumn.getIdentityGenerator();
                        if((oldSeq == null) && (newSeq != null)) {
                            addedIdentity.add(newColumn.getName());
                        } else if((oldSeq != null) && (newSeq == null)) {
                            droppedSequences.add(oldSeq.getSequenceName());
                        }
                        // else both not null and not equal, not yet supported
                    }
                } break;
                case DROP: {
                    Column oldColumn = oldTable.getColumn(change.getOldName());
                    if((oldColumn != null) && (oldColumn.getIdentityGenerator() != null)) {
                        droppedSequences.add(oldColumn.getIdentityGenerator().getSequenceName());
                    }
                } break;
            }
        }

        boolean renamed = !oldTable.getName().equals(newTable.getName()) || !renamedColumns.isEmpty();

        Map<String,String> preserveIndexes = new TreeMap<>();
        TableName parentName = (newTable.getParentJoin() != null) ? newTable.getParentJoin().getParent().getName() : null;
        changedTables.add(new ChangedTableDescription(oldTable.getName(), newTable, renamedColumns,
                                                      parentChange, parentName, EMPTY_STRING_MAP, preserveIndexes,
                                                      droppedSequences, addedIdentity));

        if(!isParentChanged() && !primaryKeyChanged) {
            for(Index index : newTable.getIndexesIncludingInternal()) {
                String oldName = index.getIndexName().getName();
                String newName = findNewName(indexChanges, oldName);
                if(!containsOldOrNew(indexChanges, oldName)) {
                    preserveIndexes.put(oldName, newName);
                }
            }
        }

        Collection<Join> oldChildJoins = new ArrayList<>(oldTable.getCandidateChildJoins());
        for(Join join : oldChildJoins) {
            UserTable oldChildTable = join.getChild();

            // If referenced column has anymore has a TABLE change (or is missing), join needs dropped
            boolean dropParent = false;
            for(JoinColumn joinCol : join.getJoinColumns()) {
                Column oldColumn = joinCol.getParent().getColumn();
                String newName = findNewName(columnChanges, oldColumn.getName());
                if(newName == null) {
                    dropParent = true;
                } else {
                    Column newColumn = newTable.getColumn(newName);
                    if(compare(oldColumn, newColumn) == ChangeLevel.TABLE) {
                        dropParent = true;
                    }
                }
            }

            boolean preserve = false;
            ParentChange change = null;

            // If PK changed and table had children, PK was dropped
            if(primaryKeyChanged || dropParent) {
                updateFinalChangeLevel(ChangeLevel.GROUP);
                change = ParentChange.DROP;
            } else if(isParentChanged() || (parentChange == ParentChange.ADD)) {
                updateFinalChangeLevel(ChangeLevel.GROUP);
                change = ParentChange.UPDATE;
            } else if(renamed) {
                updateFinalChangeLevel(ChangeLevel.METADATA);
                change = ParentChange.META;
                preserve = true;
            }

            if(change != null) {
                TableName newParent = (change == ParentChange.DROP) ? null : newTable.getName();
                trackChangedTable(oldChildTable, change, newParent, renamedColumns, preserve);
                propagateChildChange(oldChildTable, ParentChange.UPDATE, preserve);
            }
        }

        if(isParentChanged() || primaryKeyChanged) {
            updateFinalChangeLevel(ChangeLevel.GROUP);
        }
    }

    private void propagateChildChange(final UserTable table, final ParentChange change, final boolean allIndexes) {
        table.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable curTable) {
                if(table != curTable) {
                    TableName parentName = curTable.getParentJoin().getParent().getName();
                    trackChangedTable(curTable, change, parentName, null, allIndexes);
                }
            }
        });
    }

    private void trackChangedTable(UserTable table, ParentChange parentChange, TableName parentName,
                                   Map<String, String> parentRenames, boolean doPreserve) {
        Map<String,String> preserved = new HashMap<>();
        if(doPreserve) {
            for(Index index : table.getIndexesIncludingInternal()) {
                preserved.put(index.getIndexName().getName(), index.getIndexName().getName());
            }
        }
        parentRenames = (parentRenames != null) ? parentRenames : EMPTY_STRING_MAP;
        changedTables.add(new ChangedTableDescription(table.getName(), null, EMPTY_STRING_MAP,
                                                      parentChange, parentName, parentRenames, preserved,
                                                      EMPTY_TABLE_NAME_LIST, Collections.<String>emptyList()));
    }

    private static boolean containsOldOrNew(List<TableChange> changes, String name) {
        for(TableChange change : changes) {
            if(name.equals(change.getOldName()) || name.equals(change.getNewName())) {
                return true;
            }
        }
        return false;
    }

    private static String findNewName(List<TableChange> changes, String oldName) {
        for(TableChange change : changes) {
            if(oldName.equals(change.getOldName())) {
                switch(change.getChangeType()) {
                    case ADD:       throw new IllegalStateException("Old name was added? " + oldName);
                    case DROP:      return null;
                    case MODIFY:    return change.getNewName();
                }
            }
        }
        return oldName;
    }

    private <T> ChangeLevel compare(T oldVal, T newVal) {
        if(oldVal instanceof Column) {
            return compare((Column)oldVal, (Column)newVal);
        }
        if(oldVal instanceof Index) {
            return compare((Index)oldVal, (Index)newVal);
        }
        throw new IllegalStateException("Cannot compare: " + oldVal + " and " + newVal);
    }

    private static ChangeLevel compare(Column oldCol, Column newCol) {
        if(Types3Switch.ON) {
            if(!oldCol.tInstance().equalsExcludingNullable(newCol.tInstance())) {
                return ChangeLevel.TABLE;
            }
        } else {
            if(!oldCol.getType().equals(newCol.getType()) ||
               !Objects.equal(oldCol.getTypeParameter1(), newCol.getTypeParameter1()) ||
               !Objects.equal(oldCol.getTypeParameter2(), newCol.getTypeParameter2()) ||
               (oldCol.getType().usesCollator() && !Objects.equal(oldCol.getCharsetAndCollation(), newCol.getCharsetAndCollation()))) {
                return ChangeLevel.TABLE;
            }
        }
        boolean oldNull = oldCol.getNullable();
        boolean newNull = newCol.getNullable();
        if((oldNull == true) && (newNull == false)) {
            return ChangeLevel.METADATA_NOT_NULL;
        }
        if((oldNull != newNull) ||
           !oldCol.getName().equals(newCol.getName()) ||
           !Objects.equal(oldCol.getDefaultValue(), newCol.getDefaultValue()) ||
           !Objects.equal(oldCol.getDefaultIdentity(), newCol.getDefaultIdentity()) ||
           sequenceChanged(oldCol.getIdentityGenerator(), newCol.getIdentityGenerator())) {
          return ChangeLevel.METADATA;
        }
        return ChangeLevel.NONE;
    }

    private ChangeLevel compare(Index oldIndex, Index newIndex) {
        if(oldIndex.getKeyColumns().size() != newIndex.getKeyColumns().size()) {
            return ChangeLevel.INDEX;
        }

        Iterator<IndexColumn> oldIt = oldIndex.getKeyColumns().iterator();
        Iterator<IndexColumn> newIt = newIndex.getKeyColumns().iterator();
        while(oldIt.hasNext()) {
            IndexColumn oldICol = oldIt.next();
            IndexColumn newICol = newIt.next();
            String newColName = findNewName(columnChanges, oldICol.getColumn().getName());
            // Column the same?
            if((newColName == null) || !newICol.getColumn().getName().equals(newColName)) {
                return ChangeLevel.INDEX;
            }
            // IndexColumn properties
            if(!Objects.equal(oldICol.getIndexedLength(), newICol.getIndexedLength()) ||
               !Objects.equal(oldICol.isAscending(), newICol.isAscending())) {
                return ChangeLevel.INDEX;
            }
            // Column being indexed
            if(compare(oldICol.getColumn(), newICol.getColumn()) == ChangeLevel.TABLE) {
                return ChangeLevel.INDEX;
            }
        }

        if(!oldIndex.getIndexName().getName().equals(newIndex.getIndexName().getName())) {
            return ChangeLevel.METADATA;
        }
        return ChangeLevel.NONE;
    }

    private static boolean sequenceChanged(Sequence oldSeq, Sequence newSeq) {
        if(oldSeq == null && newSeq == null) {
            return false;
        }
        if(oldSeq != null && newSeq == null) {
            return true;
        }
        if(oldSeq == null /*&& newSeq != null**/) {
            return true;
        }
        return (oldSeq.getStartsWith() != newSeq.getStartsWith()) ||
               (oldSeq.getIncrement() != newSeq.getIncrement()) ||
               (oldSeq.getMinValue() != newSeq.getMinValue()) ||
               (oldSeq.getMaxValue() != newSeq.getMaxValue()) ||
               (oldSeq.isCycle() != newSeq.isCycle());
    }

    private static ParentChange compareParentJoin(List<TableChange> columnChanges, Join oldJoin, Join newJoin) {
        if(oldJoin == null && newJoin == null) {
            return ParentChange.NONE;
        }
        if(oldJoin != null && newJoin == null) {
            return ParentChange.DROP;
        }
        if(oldJoin == null /*&& newJoin != null*/) {
            return ParentChange.ADD;
        }

        UserTable oldParent = oldJoin.getParent();
        UserTable newParent = newJoin.getParent();
        if(!oldParent.getName().equals(newParent.getName()) ||
           (oldJoin.getJoinColumns().size() != newJoin.getJoinColumns().size())) {
            return ParentChange.ADD;
        }

        boolean sawRename = false;
        Iterator<JoinColumn> oldIt = oldJoin.getJoinColumns().iterator();
        Iterator<JoinColumn> newIt = newJoin.getJoinColumns().iterator();
        while(oldIt.hasNext()) {
            Column oldCol = oldIt.next().getChild();
            String newName = findNewName(columnChanges, newIt.next().getChild().getName());
            if(newName == null) {
                return ParentChange.DROP;
            } else {
                Column newCol = newJoin.getChild().getColumn(newName);
                if(compare(oldCol, newCol) == ChangeLevel.TABLE) {
                    return ParentChange.DROP;
                } else if(!oldCol.getName().equals(newName)) {
                    sawRename = true;
                }
            }
        }
        return sawRename ? ParentChange.META : ParentChange.NONE;
    }

    private void addNotPresent(boolean isIndex, TableChange change) {
        String detail = change.toString();
        errors.add(isIndex ? new AddIndexNotPresentException(detail) : new AddColumnNotPresentException(detail));
    }

    private void dropNotPresent(boolean isIndex, TableChange change) {
        String detail = change.toString();
        errors.add(isIndex ? new DropIndexNotPresentException(detail) : new DropColumnNotPresentException(detail));
    }

    private static RuntimeException modifyNotChanged(boolean isIndex, TableChange change) {
        String detail = change.toString();
        return isIndex ? new ModifyIndexNotChangedException(detail) : new ModifyColumnNotChangedException(detail);
    }

    private void modifyNotPresent(boolean isIndex, TableChange change) {
        String detail = change.toString();
        errors.add(isIndex ? new ModifyIndexNotPresentException(detail) : new ModifyColumnNotPresentException(detail));
    }

    private void unchangedNotPresent(boolean isIndex, String detail) {
        errors.add(isIndex ? new UnchangedIndexNotPresentException(detail) : new UnchangedColumnNotPresentException(detail));
    }

    private void undeclaredChange(boolean isIndex, String detail) {
        errors.add(isIndex ? new UndeclaredIndexChangeException(detail) : new UndeclaredColumnChangeException(detail));
    }
}
