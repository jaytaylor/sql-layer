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
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.server.types3.Types3Switch;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.MultipleCauseException;
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TableComparer {
    public static enum ChangeLevel {
        NONE,
        METADATA,
        METADATA_NULL,
        INDEX,
        TABLE,
        GROUP
    }

    public static class AddColumnNotPresentException extends RuntimeException {
        public AddColumnNotPresentException(String detail) {
            super("ADD column not in new table: " + detail);
        }
    }

    public static class DropColumnNotPresentException extends RuntimeException {
        public DropColumnNotPresentException(String detail) {
            super("DROP column not in old table: " + detail);
        }
    }

    public static class ModifyColumnNotPresentException extends RuntimeException {
        public ModifyColumnNotPresentException(String detail) {
            super("MODIFY column not in old or new table: " + detail);
        }
    }

    public static class ModifyColumnNotChangedException extends RuntimeException {
        public ModifyColumnNotChangedException(String detail) {
            super("MODIFY column not changed: " + detail);
        }
    }

    public static class UnchangedColumnNotPresentException extends RuntimeException {
        public UnchangedColumnNotPresentException(String detail) {
            super("Unchanged column not present in new table: " + detail);
        }
    }

    public static class UndeclaredColumnChangeException extends RuntimeException {
        public UndeclaredColumnChangeException(String detail) {
            super("Undeclared column change in new table: " + detail);
        }
    }



    private final UserTable oldTable;
    private final UserTable newTable;
    private final List<TableChange> columnChanges;
    private final List<TableChange> indexChanges;
    private final Set<ChangeLevel> columnChangeLevels;
    private final Set<ChangeLevel> indexChangeLevels;
    private final List<RuntimeException> errors;
    private ChangeLevel tableChangeLevel;
    private boolean parentChanged;
    private boolean childChanged;
    private boolean didCompare;

    public TableComparer(UserTable oldTable, UserTable newTable,
                         List<TableChange> columnChanges, List<TableChange> indexChanges) {
        ArgumentValidation.notNull("oldTable", oldTable);
        ArgumentValidation.notNull("newTable", newTable);
        this.oldTable = oldTable;
        this.newTable = newTable;
        this.columnChanges = (columnChanges == null) ? Collections.<TableChange>emptyList() : columnChanges;
        this.indexChanges = (indexChanges == null) ? Collections.<TableChange>emptyList() : indexChanges;
        this.errors = new ArrayList<RuntimeException>();
        this.columnChangeLevels = EnumSet.noneOf(ChangeLevel.class);
        this.indexChangeLevels = EnumSet.noneOf(ChangeLevel.class);
    }

    public ChangeLevel getTableChangeLevel() {
        return tableChangeLevel;
    }

    public Set<ChangeLevel> getColumnChangeLevels() {
        return columnChangeLevels;
    }

    public Set<ChangeLevel> getIndexChangeLevels() {
        return indexChangeLevels;
    }

    public boolean isParentChanged() {
        return parentChanged;
    }

    public boolean isChildChanged() {
        return childChanged;
    }

    public void compare() {
        if(!didCompare) {
            compareTable();
            compareColumns();
            compareIndexes();
            compareGrouping();
            setFinalChangeLevel();
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

    private void setFinalChangeLevel() {
        if(!errors.isEmpty()) {
            tableChangeLevel = null;
        } else {
            for(ChangeLevel level : columnChangeLevels) {
                tableChangeLevel = (level.ordinal() > tableChangeLevel.ordinal()) ? level : tableChangeLevel;
            }
            for(ChangeLevel level : indexChangeLevels) {
                tableChangeLevel = (level.ordinal() > tableChangeLevel.ordinal()) ? level : tableChangeLevel;
            }
        }
    }

    private void compareTable() {
        tableChangeLevel = oldTable.getName().equals(newTable.getName()) ? ChangeLevel.NONE : ChangeLevel.METADATA;
    }

    private void compareColumns() {
        Set<String> oldExcludes = new HashSet<String>();
        Set<String> newExcludes = new HashSet<String>();

        // Check claimed column changes
        for(TableChange change : columnChanges) {
            Column oldCol = (change.getOldName() != null) ? oldTable.getColumn(change.getOldName()) : null;
            Column newCol = (change.getNewName() != null) ? newTable.getColumn(change.getNewName()) : null;
            switch(change.getChangeType()) {
                case ADD:
                    if(newCol == null) {
                        error(new AddColumnNotPresentException(change.toString()));
                    } else {
                        columnChangeLevels.add(ChangeLevel.TABLE);
                        newExcludes.add(newCol.getName());
                    }
                break;
                case DROP:
                    if(oldCol == null) {
                        error(new DropColumnNotPresentException(change.toString()));
                    } else {
                        columnChangeLevels.add(ChangeLevel.TABLE);
                        oldExcludes.add(oldCol.getName());
                    }
                    // TODO: Check in PK
                break;
                case MODIFY:
                    if((oldCol == null) || (newCol == null)) {
                        error(new ModifyColumnNotPresentException(change.toString()));
                    } else {
                        ChangeLevel colChange = compare(oldCol, newCol);
                        if(colChange == ChangeLevel.NONE) {
                            error(new ModifyColumnNotChangedException(change.toString()));
                        } else {
                            columnChangeLevels.add(colChange);
                        }
                    }
                    if(oldCol != null) {
                        oldExcludes.add(oldCol.getName());
                    }
                    if(newCol != null) {
                        newExcludes.add(newCol.getName());
                    }
                    // TODO: Check in PK AND !metadata
                break;
                default:
                    error(new IllegalStateException("Unknown ChangeType: " + change));
            }
        }

        // Check remaining columns in old table
        for(Column oldCol : oldTable.getColumns()) {
            String colName = oldCol.getName();
            if(!oldExcludes.contains(colName)) {
                Column newCol = newTable.getColumn(colName);
                if(newCol == null) {
                    errors.add(new UnchangedColumnNotPresentException(colName));
                } else {
                    ChangeLevel change = compare(oldCol, newCol);
                    if(change != ChangeLevel.NONE) {
                        error(new UndeclaredColumnChangeException(colName));
                    }
                    newExcludes.add(colName);
                }
            }
        }

        // Check remaining columns in new table (should be none)
        for(Column newCol : newTable.getColumns()) {
            String colName = newCol.getName();
            if(!newExcludes.contains(colName)) {
                error(new UndeclaredColumnChangeException(colName));
            }
        }
    }

    private void compareIndexes() {
    }

    private void compareGrouping() {
        // Note: PK (grouping) changes checked in compareColumns()
        parentChanged = compare(oldTable.getParentJoin(), newTable.getParentJoin());
    }

    private void error(RuntimeException e) {
        errors.add(e);
    }

    private static ChangeLevel compare(Column oldCol, Column newCol) {
        if(Types3Switch.ON) {
            if(!oldCol.tInstance().equalsExcludingNullable(newCol.tInstance())) {
                return ChangeLevel.TABLE;
            }
        } else {
            if(!oldCol.getType().equals(newCol.getType()) ||
               !Objects.equal(oldCol.getTypeParameter1(), oldCol.getTypeParameter2()) ||
               !Objects.equal(oldCol.getTypeParameter1(), oldCol.getTypeParameter2()) ||
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

    /** false = same, true = different **/
    private static boolean compare(Join oldJoin, Join newJoin) {
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
}
