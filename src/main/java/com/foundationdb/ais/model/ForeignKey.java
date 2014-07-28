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

import com.foundationdb.ais.model.validation.AISInvariants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ForeignKey implements Constraint
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
                                    Action updateAction,
                                    boolean deferrable,
                                    boolean initiallyDeferred) {
        ais.checkMutability();
        ForeignKey fk = new ForeignKey(constraintName,
                                       referencingTable, referencingColumns,
                                       referencedTable, referencedColumns,
                                       deleteAction, updateAction,
                                       deferrable, initiallyDeferred);
        AISInvariants.checkDuplicateConstraintsInSchema(ais, fk.getConstraintName());
        referencingTable.addForeignKey(fk);
        referencedTable.addForeignKey(fk);
        if (constraintName != null) {
            ais.addConstraint(fk);
        }
        return fk;
    }

    public void findIndexes() {
        if (referencingIndex == null) {
            referencingIndex = join.getChild().getIndex(constraintName.getTableName());
        }
        if (referencedIndex == null) {
            referencedIndex = findReferencedIndex(join.getParent(), join.getParentColumns());
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

    public TableName getConstraintName() {
        return constraintName;
    }

    public Table getReferencingTable() {
        return join.getChild();
    }

    public List<Column> getReferencingColumns() {
        return join.getChildColumns();
    }

    public TableIndex getReferencingIndex() {
        return referencingIndex;
    }

    public Table getReferencedTable() {
        return join.getParent();
    }

    public List<Column> getReferencedColumns() {
        return join.getParentColumns();
    }

    public TableIndex getReferencedIndex() {
        return referencedIndex;
    }

    public List<JoinColumn> getJoinColumns() {
        return join.getJoinColumns();
    }
    
    public Action getDeleteAction() {
        return deleteAction;
    }

    public Action getUpdateAction() {
        return updateAction;
    }
    
    public boolean isDeferrable() {
        return deferrable;
    }

    public boolean isInitiallyDeferred() {
        return initiallyDeferred;
    }

    public boolean isDeferred(Map<ForeignKey,Boolean> transactionDeferred) {
        if (!deferrable)
            return false;
        if (transactionDeferred != null) {
            Boolean result = transactionDeferred.get(null);
            if (result != null)
                return result;
            result = transactionDeferred.get(this);
            if (result != null)
                return result;
        }
        return initiallyDeferred;
    }

    public static Map<ForeignKey,Boolean> setDeferred(Map<ForeignKey,Boolean> transactionDeferred,
                                                      ForeignKey fkey, boolean deferred) {
        if (transactionDeferred == null)
            transactionDeferred = new HashMap<>();
        if (fkey == null) {
            transactionDeferred.clear();
        } else if(!fkey.isDeferrable()) {
            throw new IllegalArgumentException("Not deferrable: " + fkey);
        }
        transactionDeferred.put(fkey, deferred);
        return transactionDeferred;
    }

    @Override
    public String toString()
    {
        return "Foreign Key " + constraintName.getTableName() + ": " + join.getChild() + " REFERENCES " + join.getParent(); 
    }

    private ForeignKey(String constraintName,
                       Table referencingTable,
                       List<Column> referencingColumns,
                       Table referencedTable,
                       List<Column> referencedColumns,
                       Action deleteAction,
                       Action updateAction,
                       boolean deferrable,
                       boolean initiallyDeferred) {
        this.constraintName = new TableName(referencingTable.getName().getSchemaName(), constraintName);
        this.deleteAction = deleteAction;
        this.updateAction = updateAction;
        this.deferrable = deferrable;
        this.initiallyDeferred = initiallyDeferred;
        
        join = Join.create(constraintName, referencedTable, referencingTable);
        for (int i = 0; i < referencingColumns.size(); i++) {
            join.addJoinColumn(referencedColumns.get(i), referencingColumns.get(i));
        }
        
    }

    // NOTE: referencingColumns (join#childColumns) and 
    // referencedColumns (join#parentColumns)  are in
    // declaration order and parallel to one another. They are not
    // necessarily in the order of referencingIndex or
    // referencedIndex.

    private final TableName constraintName;
    private final Action deleteAction;
    private final Action updateAction;
    private final boolean deferrable, initiallyDeferred;
    private TableIndex referencingIndex;
    private TableIndex referencedIndex;
    private Join join;

}
