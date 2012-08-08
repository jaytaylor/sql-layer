/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.util;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.NopVisitor;
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
import java.util.TreeSet;

import static com.akiban.ais.util.TableChangeValidatorException.*;

public class TableChangeValidator {
    public static enum ChangeLevel {
        NONE,
        METADATA,
        METADATA_NULL,
        INDEX,
        TABLE,
        GROUP
    }

    private final UserTable oldTable;
    private final UserTable newTable;
    private final List<TableChange> columnChanges;
    private final List<TableChange> indexChanges;
    private final List<RuntimeException> errors;
    private final Collection<ChangedTableDescription> changedTables;
    private final Collection<IndexName> autoAffectedGroupIndexes;
    private ChangeLevel finalChangeLevel;
    private boolean parentChanged;
    private boolean primaryKeyChanged;
    private boolean didCompare;

    public TableChangeValidator(UserTable oldTable, UserTable newTable,
                                List<TableChange> columnChanges, List<TableChange> indexChanges) {
        ArgumentValidation.notNull("oldTable", oldTable);
        ArgumentValidation.notNull("newTable", newTable);
        this.oldTable = oldTable;
        this.newTable = newTable;
        this.columnChanges = (columnChanges == null) ? Collections.<TableChange>emptyList() : columnChanges;
        this.indexChanges = (indexChanges == null) ? Collections.<TableChange>emptyList() : indexChanges;
        this.errors = new ArrayList<RuntimeException>();
        this.changedTables = new ArrayList<ChangedTableDescription>();
        this.autoAffectedGroupIndexes = new TreeSet<IndexName>();
        this.finalChangeLevel = ChangeLevel.NONE;
    }

    public ChangeLevel getFinalChangeLevel() {
        return finalChangeLevel;
    }

    public Collection<ChangedTableDescription> getAllChangedTables() {
        return changedTables;
    }

    public Collection<IndexName> getAutoAffectedGroupIndexes() {
        return autoAffectedGroupIndexes;
    }

    public boolean isParentChanged() {
        return parentChanged;
    }

    public boolean isPrimaryKeyChanged() {
        return primaryKeyChanged;
    }

    public void compare() {
        if(!didCompare) {
            compareTable();
            compareColumns();
            compareIndexes();
            compareGrouping();
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
        if(!oldName.equals(newName)) {
            updateFinalChangeLevel(ChangeLevel.METADATA);
        }
    }

    private void compareColumns() {
        Map<String,Column> oldColumns = new HashMap<String,Column>();
        Map<String,Column> newColumns = new HashMap<String,Column>();
        for(Column column : oldTable.getColumns()) {
            oldColumns.put(column.getName(), column);
        }
        for(Column column : newTable.getColumns()) {
            newColumns.put(column.getName(), column);
        }
        checkChanges(ChangeLevel.TABLE, columnChanges, oldColumns, newColumns);
    }

    private void compareIndexes() {
        Map<String,Index> oldIndexes = new HashMap<String,Index>();
        Map<String,Index> newIndexes = new HashMap<String,Index>();
        for(Index index : oldTable.getIndexes()) {
            oldIndexes.put(index.getIndexName().getName(), index);
        }
        for(Index index : newTable.getIndexes()) {
            newIndexes.put(index.getIndexName().getName(), index);
        }
        checkChanges(ChangeLevel.INDEX, indexChanges, oldIndexes, newIndexes);
    }

    private <T> void checkChanges(ChangeLevel level, List<TableChange> changeList, Map<String,T> oldMap, Map<String,T> newMap) {
        final boolean isIndex = (level == ChangeLevel.INDEX);
        Set<String> oldExcludes = new HashSet<String>();
        Set<String> newExcludes = new HashSet<String>();

        // Check declared changes
        for(TableChange change : changeList) {
            String oldName = change.getOldName();
            String newName = change.getNewName();
            switch(change.getChangeType()) {
                case ADD: {
                    if(newMap.get(newName) == null) {
                        addNotPresent(isIndex, change);
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
                            modifyNotChanged(isIndex, change);
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
                    unchangedNotPresent(isIndex, name);
                } else {
                    ChangeLevel change = compare(entry.getValue(), newVal);
                    if(change != ChangeLevel.NONE) {
                        undeclaredChange(isIndex, name);
                    }
                    newExcludes.add(name);
                }
            }
        }

        // Check remaining elements in new table (should be none)
        for(String name : newMap.keySet()) {
            if(!newExcludes.contains(name)) {
                undeclaredChange(isIndex, name);
            }
        }
    }

    private void compareGrouping() {
        parentChanged = joinChanged(oldTable.getParentJoin(), newTable.getParentJoin());
        primaryKeyChanged = containsOldOrNew(indexChanges, Index.PRIMARY_KEY_CONSTRAINT);

        Map<String,String> preserveIndexes = new TreeMap<String,String>();
        changedTables.add(new ChangedTableDescription(oldTable.getName(), newTable, parentChanged, preserveIndexes));

        if(!parentChanged && !primaryKeyChanged) {
            for(Index index : newTable.getIndexes()) {
                String oldName = index.getIndexName().getName();
                String newName = findNewName(indexChanges, oldName);
                if(!containsOldOrNew(indexChanges, oldName)) {
                    preserveIndexes.put(oldName, newName);
                }
            }
        }

        for(Join join : newTable.getChildJoins()) {
            UserTable childTable = join.getChild();
            childTable.removeCandidateParentJoin(join);

            Join newJoin = Join.create(childTable.getAIS(), join.getName(), newTable, childTable);
            newJoin.setGroup(newTable.getGroup());
            for(JoinColumn joinCol : join.getJoinColumns()) {
                String oldColName = joinCol.getParent().getColumn().getName();
                String newName = findNewName(columnChanges, oldColName);
                if(newName != null) {
                    JoinColumn.create(newJoin, newTable.getColumn(newName), joinCol.getChild());
                }
            }

            if(primaryKeyChanged || (newJoin.getJoinColumns().size() == 0)) {
                childTable.removeCandidateParentJoin(newJoin);
                propagateChange(childTable, true);
            }
            else if(newJoin.getJoinColumns().size() != join.getJoinColumns().size()) {
                propagateChange(childTable, false);
            }
        }

        if(parentChanged || primaryKeyChanged || (changedTables.size() > 1)) {
            updateFinalChangeLevel(ChangeLevel.GROUP);
        }
    }

    private void propagateChange(final UserTable newRoot, final boolean withNewGroup) {
        if(withNewGroup) {
            Group newGroup = Group.create(newRoot.getAIS(), newRoot.getName() + "_temp");
            GroupTable groupTable = GroupTable.create(newRoot.getAIS(), newRoot.getName().getSchemaName(), "temp", 0);
            groupTable.setGroup(newGroup);
            newGroup.setGroupTable(groupTable);
            newRoot.setGroup(newGroup);
        }

        newRoot.traverseTableAndDescendents(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable curTable) {
                trackChangedChild(newRoot);

                if(withNewGroup && (curTable != newRoot)) {
                    // Rebuild join
                    curTable.setGroup(newRoot.getGroup());
                    Join oldJoin = curTable.getParentJoin();
                    curTable.removeCandidateParentJoin(oldJoin);

                    Join newJoin = Join.create(curTable.getAIS(), oldJoin.getName(), oldJoin.getParent(),
                                               oldJoin.getChild());
                    newJoin.setGroup(newRoot.getGroup());
                    for(JoinColumn joinColumn : oldJoin.getJoinColumns()) {
                        JoinColumn.create(newJoin,
                                          oldJoin.getParent().getColumn(joinColumn.getParent().getName()),
                                          oldJoin.getChild().getColumn(joinColumn.getChild().getName()));
                    }
                }
            }
        });
    }

    private void trackChangedChild(UserTable table) {
        changedTables.add(new ChangedTableDescription(table.getName(), table, true, new HashMap<String, String>()));
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
                    case DROP:
                        return null;
                    case MODIFY:
                        return change.getNewName();
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
        if(!oldCol.getNullable().equals(newCol.getNullable())) {
            return ChangeLevel.METADATA_NULL;
        }
        if(!oldCol.getName().equals(newCol.getName()) ||
           !Objects.equal(oldCol.getIdentityGenerator(), newCol.getIdentityGenerator())) {
          return ChangeLevel.METADATA;
        }

        // TODO: Check defaults

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

    private static boolean joinChanged(Join oldJoin, Join newJoin) {
        if(oldJoin == null && newJoin == null) {
            return false;
        }
        if(oldJoin != null && newJoin == null) {
            return true;
        }
        if(oldJoin == null /*&& newJoin != null*/) {
            return true;
        }

        UserTable oldParent = oldJoin.getParent();
        UserTable newParent = newJoin.getParent();
        if(!oldParent.getName().equals(newParent.getName()) ||
           (oldJoin.getJoinColumns().size() != newJoin.getJoinColumns().size())) {
            return true;
        }

        Iterator<JoinColumn> oldIt = oldJoin.getJoinColumns().iterator();
        Iterator<JoinColumn> newIt = newJoin.getJoinColumns().iterator();
        while(oldIt.hasNext()) {
            JoinColumn oldCol = oldIt.next();
            JoinColumn newCol = newIt.next();
            if(!oldCol.getParent().getName().equals(newCol.getParent().getName()) ||
               !oldCol.getChild().getName().equals(newCol.getChild().getName())) {
                return true;
            }
        }
        return false;
    }

    private void addNotPresent(boolean isIndex, TableChange change) {
        String detail = change.toString();
        errors.add(isIndex ? new AddIndexNotPresentException(detail) : new AddColumnNotPresentException(detail));
    }

    private void dropNotPresent(boolean isIndex, TableChange change) {
        String detail = change.toString();
        errors.add(isIndex ? new DropIndexNotPresentException(detail) : new DropColumnNotPresentException(detail));
    }

    private void modifyNotChanged(boolean isIndex, TableChange change) {
        String detail = change.toString();
        errors.add(isIndex ? new ModifyIndexNotChangedException(detail) : new ModifyColumnNotChangedException(detail));
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
