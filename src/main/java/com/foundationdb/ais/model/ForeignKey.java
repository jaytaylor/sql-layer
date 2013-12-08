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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ForeignKey
{
    public static enum Action {
        NO_ACTION, RESTRICT, CASCADE, SET_NULL, SET_DEFAULT;

        public String toSQL() {
            return name().replace('_', ' ');
        }
    }

    public static ForeignKey create(AkibanInformationSchema ais,
                                    String constraintName,
                                    Table referencingTable,
                                    List<Column> referencingColumns,
                                    Table referencedTable,
                                    List<Column> referencedColumns,
                                    Action deleteAction,
                                    Action updateAction) {
        ais.checkMutability();
        ForeignKey fk = new ForeignKey(constraintName,
                                       referencingTable, referencingColumns,
                                       referencedTable, referencedColumns,
                                       deleteAction, updateAction);
        referencingTable.addForeignKey(fk);
        referencedTable.addForeignKey(fk);
        return fk;
    }

    public void findIndexes() {
        if (referencingIndex == null) {
            referencingIndex = referencingTable.getIndex(constraintName);
            referencingIndex.setConstraint(Index.FOREIGN_KEY_CONSTRAINT);
        }
        if (referencedIndex == null) {
            referencedIndex = findReferencedIndex(referencedTable, referencedColumns);
        }
    }

    /** Find a unique index on <code>referencedTable</code> with
     * <code>referencedColumns</code> in some order.
     */
    public static TableIndex findReferencedIndex(Table referencedTable,
                                                 List<Column> referencedColumns) {
        int ncols = referencedColumns.size();
        for (TableIndex index : referencedTable.getIndexesIncludingInternal()) {
            if (!(index.isUnique() && (index.getKeyColumns().size() == ncols)))
                continue;
            boolean found = true;
            for (int i = 0; i < ncols; i++) {
                if (referencedColumns.indexOf(index.getKeyColumns().get(i).getColumn()) < 0) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return index;
            }
        }
        return null;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public Table getReferencingTable() {
        return referencingTable;
    }

    public List<Column> getReferencingColumns() {
        return unmodifiableReferencingColumns;
    }

    public TableIndex getReferencingIndex() {
        return referencingIndex;
    }

    public Table getReferencedTable() {
        return referencedTable;
    }

    public List<Column> getReferencedColumns() {
        return unmodifiableReferencedColumns;
    }

    public TableIndex getReferencedIndex() {
        return referencedIndex;
    }

    public Action getDeleteAction() {
        return deleteAction;
    }

    public Action getUpdateAction() {
        return updateAction;
    }

    private ForeignKey(String constraintName,
                       Table referencingTable,
                       List<Column> referencingColumns,
                       Table referencedTable,
                       List<Column> referencedColumns,
                       Action deleteAction,
                       Action updateAction) {
        this.constraintName = constraintName;
        this.referencingTable = referencingTable;
        this.referencingColumns = new ArrayList<>(referencingColumns);
        this.unmodifiableReferencingColumns = Collections.unmodifiableList(this.referencingColumns);
        this.referencedTable = referencedTable;
        this.referencedColumns = new ArrayList<>(referencedColumns);
        this.unmodifiableReferencedColumns = Collections.unmodifiableList(this.referencedColumns);
        this.deleteAction = deleteAction;
        this.updateAction = updateAction;
    }

    // NOTE: referencingColumns and referencedColumns are in
    // declaration order and parallel to one another. They are not
    // necessarily in the order of referencingIndex or
    // referencedIndex.

    private final String constraintName;
    private final Table referencingTable;
    private final List<Column> referencingColumns;
    private final List<Column> unmodifiableReferencingColumns;
    private final Table referencedTable;
    private final List<Column> referencedColumns;
    private final List<Column> unmodifiableReferencedColumns;
    private final Action deleteAction;
    private final Action updateAction;
    private TableIndex referencingIndex;
    private TableIndex referencedIndex;

}
