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

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.ColumnName;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.util.ArgumentValidation;
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

import static com.foundationdb.ais.util.ChangedTableDescription.ParentChange;
import static com.foundationdb.ais.util.TableChangeValidatorException.*;

public class TableChangeValidator {
    private static final Map<String,String> EMPTY_STRING_MAP = Collections.emptyMap();
    private static final List<TableName> EMPTY_TABLE_NAME_LIST = Collections.emptyList();

    public static enum ChangeLevel {
        NONE,
        METADATA,
        METADATA_CONSTRAINT,
        INDEX,
        // TODO: Ugly. Final change level should be a Set and/or constraint changes passed separately.
        INDEX_CONSTRAINT,
        TABLE,
        GROUP
    }

    private final Table oldTable;
    private final Table newTable;
    private final TableChangeValidatorState state;
    private final List<RuntimeException> unmodifiedChanges;

    private ChangeLevel finalChangeLevel;
    private ChangedTableDescription.ParentChange parentChange;
    private boolean primaryKeyChanged;
    private boolean didCompare;

    public TableChangeValidator(Table oldTable,
                                Table newTable,
                                List<TableChange> columnChanges,
                                List<TableChange> tableIndexChanges) {
        ArgumentValidation.notNull("oldTable", oldTable);
        ArgumentValidation.notNull("newTable", newTable);
        ArgumentValidation.notNull("columnChanges", columnChanges);
        ArgumentValidation.notNull("tableIndexChanges", tableIndexChanges);
        this.oldTable = oldTable;
        this.newTable = newTable;
        this.state = new TableChangeValidatorState(columnChanges, tableIndexChanges);
        this.unmodifiedChanges = new ArrayList<>();
        this.finalChangeLevel = ChangeLevel.NONE;
        this.parentChange = ParentChange.NONE;
    }

    public ChangeLevel getFinalChangeLevel() {
        return finalChangeLevel;
    }

    public TableChangeValidatorState getState() {
        return state;
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
            compareIndexes();
            compareGrouping();
            compareGroupIndexes();
            compareForeignKeys();
            updateFinalChangeLevel(ChangeLevel.NONE);
            checkFinalChangeLevel();
            didCompare = true;
        }
    }

    private void updateFinalChangeLevel(ChangeLevel level) {
        if(level.ordinal() > finalChangeLevel.ordinal()) {
            finalChangeLevel = level;
        }
    }

    private void compareTable() {
        TableName oldName = oldTable.getName();
        TableName newName = newTable.getName();
        if(!oldName.equals(newName) ||
           !Objects.equal(oldTable.getCharsetName(), newTable.getCharsetName()) ||
           !Objects.equal(oldTable.getCollationName(), newTable.getCollationName())) {
            updateFinalChangeLevel(ChangeLevel.METADATA);
        }
    }

    private void compareColumns() {
        Map<String,Column> oldColumns = new HashMap<>();
        Map<String,Column> newColumns = new HashMap<>();
        for(Column column : oldTable.getColumnsIncludingInternal()) {
            oldColumns.put(column.getName(), column);
        }
        for(Column column : newTable.getColumnsIncludingInternal()) {
            newColumns.put(column.getName(), column);
        }
        checkChanges(ChangeLevel.TABLE, state.columnChanges, oldColumns, newColumns, false);

        // Look for position changes, not required to be declared
        for(Map.Entry<String, Column> oldEntry : oldColumns.entrySet()) {
            Column newColumn = newColumns.get(findNewName(state.columnChanges, oldEntry.getKey()));
            if((newColumn != null) && !oldEntry.getValue().getPosition().equals(newColumn.getPosition())) {
                updateFinalChangeLevel(ChangeLevel.TABLE);
                break;
            }
        }
    }

    private void compareIndexes() {
        Map<String,TableIndex> oldIndexes = new HashMap<>();
        Map<String,TableIndex> newIndexes = new HashMap<>();
        for(TableIndex index : oldTable.getIndexesIncludingInternal()) {
            oldIndexes.put(index.getIndexName().getName(), index);
        }

        // Look for incompatible spatial changes
        for(TableIndex oldIndex : oldTable.getIndexesIncludingInternal()) {
            String oldName = oldIndex.getIndexName().getName();
            String newName = findNewName(state.tableIndexChanges, oldName);
            TableIndex newIndex = (newName != null) ? newTable.getIndexIncludingInternal(newName) : null;
            if((newIndex != null) && oldIndex.isSpatial() && !Index.isSpatialCompatible(newIndex)) {
                newTable.removeIndexes(Collections.singleton(newIndex));

                // Remove any entry that already exists (e.g. MODIFY from compareColumns())
                Iterator<TableChange> it = state.tableIndexChanges.iterator();
                while(it.hasNext()) {
                    TableChange c = it.next();
                    if(oldName.equals(c.getOldName())) {
                        it.remove();
                        break;
                    }
                }
            }
        }

        for(TableIndex index : newTable.getIndexesIncludingInternal()) {
            newIndexes.put(index.getIndexName().getName(), index);
        }
        checkChanges(ChangeLevel.INDEX, state.tableIndexChanges, oldIndexes, newIndexes, true);
    }

    private void compareGroupIndexes() {
        final Set<Table> keepTables = new HashSet<>();
        final Table traverseStart;
        if(parentChange == ParentChange.DROP) {
            traverseStart = oldTable;
        } else {
           traverseStart = oldTable.getGroup().getRoot();
        }

        traverseStart.visit(new AbstractVisitor() {
            @Override
            public void visit(Table table) {
                keepTables.add(table);
            }
        });

        for(GroupIndex index : oldTable.getGroupIndexes()) {
            boolean dataChange = (finalChangeLevel == ChangeLevel.GROUP);
            List<ColumnName> remainingCols = new ArrayList<>();
            for(IndexColumn iCol : index.getKeyColumns()) {
                Column column = iCol.getColumn();
                if(!keepTables.contains(column.getTable())) {
                    remainingCols.clear();
                    break;
                }
                String oldName = column.getName();
                boolean isTargetTable = column.getTable() == oldTable;
                String newName = isTargetTable ? findNewName(state.columnChanges, oldName) : oldName;
                if(newName != null) {
                    TableName tableName = isTargetTable ? newTable.getName() : column.getTable().getName();
                    remainingCols.add(new ColumnName(tableName, newName));
                    if(column.getTable() == oldTable) {
                        Column oldColumn = oldTable.getColumn(oldName);
                        Column newColumn = newTable.getColumn(newName);
                        dataChange |= (compare(oldColumn, newColumn) == ChangeLevel.TABLE);
                    }
                } else {
                    dataChange = true;
                }
            }
            if(remainingCols.size() <= 1) {
                remainingCols.clear();
                state.droppedGI.add(index.getIndexName().getName());
            } else {
                state.affectedGI.put(index.getIndexName().getName(), remainingCols);
                if(dataChange) {
                    state.dataAffectedGI.put(index.getIndexName().getName(), remainingCols);
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
                        dropNotPresent(isIndex, change);
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
                        modifyNotPresent(isIndex, change);
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
                    autoChanges.add(TableChange.createAdd(name));
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
        parentChange = compareParentJoin(state.columnChanges, oldTable.getParentJoin(), newTable.getParentJoin());
        primaryKeyChanged = containsOldOrNew(state.tableIndexChanges, Index.PRIMARY_KEY_CONSTRAINT);

        List<TableName> droppedSequences = new ArrayList<>();
        List<String> addedIdentity = new ArrayList<>();
        Map<String,String> renamedColumns = new HashMap<>();
        for(TableChange change : state.columnChanges) {
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
                case ADD: {
                    Column newColumn = newTable.getColumn(change.getNewName());
                    Sequence newSeq = newColumn.getIdentityGenerator();
                    if(newSeq != null) {
                        addedIdentity.add(newColumn.getName());
                    }
                } break;
            }
        }

        boolean renamed = !oldTable.getName().equals(newTable.getName()) || !renamedColumns.isEmpty();

        Map<String,String> preserveIndexes = new TreeMap<>();
        TableName parentName = (newTable.getParentJoin() != null) ? newTable.getParentJoin().getParent().getName() : null;
        state.descriptions.add(
            new ChangedTableDescription(
                oldTable.getTableId(),
                oldTable.getName(),
                newTable,
                renamedColumns,
                parentChange,
                parentName,
                EMPTY_STRING_MAP,
                preserveIndexes,
                droppedSequences,
                addedIdentity,
                finalChangeLevel == ChangeLevel.TABLE,
                isParentChanged() || primaryKeyChanged
            )
        );

        if(!isParentChanged() && !primaryKeyChanged) {
            for(Index index : newTable.getIndexesIncludingInternal()) {
                String oldName = index.getIndexName().getName();
                String newName = findNewName(state.tableIndexChanges, oldName);
                if(!containsOldOrNew(state.tableIndexChanges, oldName)) {
                    preserveIndexes.put(oldName, newName);
                }
            }
        }

        Collection<Join> oldChildJoins = new ArrayList<>(oldTable.getCandidateChildJoins());
        for(Join join : oldChildJoins) {
            Table oldChildTable = join.getChild();

            // If referenced column has anymore has a TABLE change (or is missing), join needs dropped
            boolean dropParent = false;
            for(JoinColumn joinCol : join.getJoinColumns()) {
                Column oldColumn = joinCol.getParent().getColumn();
                String newName = findNewName(state.columnChanges, oldColumn.getName());
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

    private void compareForeignKeys() {
        // Flag referenced table as having metadata changed
        // No way to rename or alter a FK definition so only need to check presence change.
        Set<TableName> referencedChanges = new HashSet<>();
        for(ForeignKey fk : oldTable.getReferencingForeignKeys()) {
            if(newTable.getReferencingForeignKey(fk.getConstraintName().getTableName()) == null) {
                referencedChanges.add(fk.getReferencedTable().getName());
            }
        }
        for(ForeignKey fk : newTable.getReferencingForeignKeys()) {
            if(oldTable.getReferencingForeignKey(fk.getConstraintName().getTableName()) == null) {
                referencedChanges.add(fk.getReferencedTable().getName());
            }
        }
        // TODO: Would be nice to track complete details (e.g. constraint name) instead of just table
        for(TableName refName : referencedChanges) {
            if(!state.hasOldTable(refName)) {
                Table table = oldTable.getAIS().getTable(refName);
                TableName parentName = (table.getParentJoin() != null) ? table.getParentJoin().getParent().getName() : null;
                trackChangedTable(table, ParentChange.NONE, parentName, null, true);
            }
        }
        if(!referencedChanges.isEmpty()) {
            switch(finalChangeLevel) {
                case NONE:
                case METADATA:
                case METADATA_CONSTRAINT:
                    updateFinalChangeLevel(ChangeLevel.METADATA_CONSTRAINT);
                    break;
                case INDEX:
                    if(!state.dataAffectedGI.isEmpty()) {
                        throw new IllegalStateException("New FOREIGN KEY and group index?");
                    }
                    updateFinalChangeLevel(ChangeLevel.INDEX_CONSTRAINT);
                case TABLE:
                case GROUP:
                    // None. These already have constraints checked.
                    break;
                default:
                    assert false : finalChangeLevel;
            }
        }
    }

    private void propagateChildChange(final Table table, final ParentChange change, final boolean allIndexes) {
        table.visitBreadthFirst(new AbstractVisitor() {
            @Override
            public void visit(Table curTable) {
                if(table != curTable) {
                    TableName parentName = curTable.getParentJoin().getParent().getName();
                    trackChangedTable(curTable, change, parentName, null, allIndexes);
                }
            }
        });
    }

    private void trackChangedTable(Table table, ParentChange parentChange, TableName parentName,
                                   Map<String, String> parentRenames, boolean doPreserve) {
        Map<String,String> preserved = new HashMap<>();
        if(doPreserve) {
            for(Index index : table.getIndexesIncludingInternal()) {
                preserved.put(index.getIndexName().getName(), index.getIndexName().getName());
            }
        }
        parentRenames = (parentRenames != null) ? parentRenames : EMPTY_STRING_MAP;
        state.descriptions.add(
            new ChangedTableDescription(
                table.getTableId(),
                table.getName(),
                null,
                EMPTY_STRING_MAP,
                parentChange,
                parentName,
                parentRenames,
                preserved,
                EMPTY_TABLE_NAME_LIST,
                Collections.<String>emptyList(),
                false,
                !doPreserve
            )
        );
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
        if(!oldCol.getType().equalsExcludingNullable(newCol.getType())) {
            return ChangeLevel.TABLE;
        }
        boolean oldNull = oldCol.getNullable();
        boolean newNull = newCol.getNullable();
        if((oldNull == true) && (newNull == false)) {
            return ChangeLevel.METADATA_CONSTRAINT;
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
            String newColName = findNewName(state.columnChanges, oldICol.getColumn().getName());
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

        Table oldParent = oldJoin.getParent();
        Table newParent = newJoin.getParent();
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
        throw isIndex ? new AddIndexNotPresentException(detail) : new AddColumnNotPresentException(detail);
    }

    private void dropNotPresent(boolean isIndex, TableChange change) {
        String detail = change.toString();
        throw isIndex ? new DropIndexNotPresentException(detail) : new DropColumnNotPresentException(detail);
    }

    private static RuntimeException modifyNotChanged(boolean isIndex, TableChange change) {
        String detail = change.toString();
        return isIndex ? new ModifyIndexNotChangedException(detail) : new ModifyColumnNotChangedException(detail);
    }

    private void modifyNotPresent(boolean isIndex, TableChange change) {
        String detail = change.toString();
        throw isIndex ? new ModifyIndexNotPresentException(detail) : new ModifyColumnNotPresentException(detail);
    }

    private void unchangedNotPresent(boolean isIndex, String detail) {
        assert !isIndex;
        throw new UnchangedColumnNotPresentException(detail);
    }

    private void undeclaredChange(boolean isIndex, String detail) {
        assert !isIndex;
        throw new UndeclaredColumnChangeException(detail);
    }

    private void checkFinalChangeLevel() {
        if(finalChangeLevel == null) {
            return;
        }
        // Internal consistency checks
        switch(finalChangeLevel) {
            case NONE:
                if(!state.affectedGI.isEmpty()) {
                    throw new IllegalStateException("NONE but had affected GI: " + state.affectedGI);
                }
            break;
            case METADATA:
            case METADATA_CONSTRAINT:
                if(!state.droppedGI.isEmpty()) {
                    throw new IllegalStateException("META but had dropped GI: " + state.droppedGI);
                }
                if(!state.dataAffectedGI.isEmpty()) {
                    throw new IllegalStateException("META but had data affected GI: " + state.dataAffectedGI);
                }
            break;
            case INDEX:
            case INDEX_CONSTRAINT:
                if(!state.dataAffectedGI.isEmpty()) {
                    throw new IllegalStateException("INDEX but had data affected GI: " + state.dataAffectedGI);
                }
                break;
            case TABLE:
            case GROUP:
                break;
            default:
                throw new IllegalStateException(finalChangeLevel.toString());
        }
    }
}
