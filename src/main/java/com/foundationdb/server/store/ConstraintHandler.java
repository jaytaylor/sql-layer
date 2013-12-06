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

package com.foundationdb.server.store;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.exec.UpdatePlannable;
import com.foundationdb.server.PersistitKeyValueTarget;
import com.foundationdb.server.error.ForeignKeyReferencedViolationException;
import com.foundationdb.server.error.ForeignKeyReferencingViolationException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDataValueSource;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.util.AkibanAppender;

import com.persistit.Key;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handle constraint (foreign key only at present) implications of
 * basic <code>Store</code> operations.
 */
public abstract class ConstraintHandler<SType extends AbstractStore,SDType,SSDType>
    implements CacheValueGenerator<Map<Table,ConstraintHandler.Handler>>
{
    protected final SType store;

    protected ConstraintHandler(SType store) {
        this.store = store;
    }

    public void handleInsert(Session session, Table table, RowData row) {
        Handler th = getTableHandler(table);
        if (th != null) {
            th.handleInsert(session, row);
        }
    }

    public void handleUpdate(Session session, Table table,
                             RowData oldRow, RowData newRow) {
        Handler th = getTableHandler(table);
        if (th != null) {
            th.handleUpdate(session, oldRow, newRow);
        }
    }

    public void handleDelete(Session session, Table table, RowData row) {
        Handler th = getTableHandler(table);
        if (th != null) {
            th.handleDelete(session, row);
        }
    }

    public void handleTruncate(Session session, Table table) {
        Handler th = getTableHandler(table);
        if (th != null) {
            th.handleTruncate(session);
        }
    }
    
    protected Handler getTableHandler(Table table) {
        Collection<ForeignKey> fkeys = table.getForeignKeys();
        if (fkeys.isEmpty()) {
            // Fast check for no constraints; don't bother with per-table handler.
            return null;
        }
        Map<Table,Handler> handlers = table.getAIS().getCachedValue(this, this);
        synchronized (handlers) {
            Handler handler = handlers.get(table);
            if (handler == null) {
                handler = createHandler(table, fkeys);
                handlers.put(table, handler);
            }
            return handler;
        }
    }

    @Override
    public Map<Table,Handler> valueFor(AkibanInformationSchema ais) {
        return new HashMap<>();
    }

    protected Handler createHandler(Table table, Collection<ForeignKey> fkeys) {
        switch (fkeys.size()) {
        case 0:
            return null;
        case 1:
            return new ForeignKeyHandler(fkeys.iterator().next(), table);
        default:
            Collection<Handler> handlers = new ArrayList<>(fkeys.size());
            for (ForeignKey fkey : fkeys) {
                handlers.add(new ForeignKeyHandler(fkey, table));
            }
            return new CompoundHandler(handlers);
        }
    }

    protected interface Handler {
        public void handleInsert(Session session, RowData row);
        public void handleUpdate(Session session, RowData oldRow, RowData newRow);
        public void handleDelete(Session session, RowData row);
        public void handleTruncate(Session session);
    }

    protected class CompoundHandler implements Handler {
        protected final Collection<Handler> handlers;

        public CompoundHandler(Collection<Handler> handlers) {
            this.handlers = handlers;
        }

        public void handleInsert(Session session, RowData row) {
            for (Handler handler : handlers) {
                handler.handleInsert(session, row);
            }
        }

        public void handleUpdate(Session session, RowData oldRow, RowData newRow) {
            for (Handler handler : handlers) {
                handler.handleUpdate(session, oldRow, newRow);
            }
        }

        public void handleDelete(Session session, RowData row) {
            for (Handler handler : handlers) {
                handler.handleDelete(session, row);
            }
        }

        public void handleTruncate(Session session) {
            for (Handler handler : handlers) {
                handler.handleTruncate(session);
            }
        }
    }

    protected static List<Column> crossReferenceColumns(ForeignKey fkey, 
                                                        boolean referencing) {
        List <Column> rowColumns, indexColumns;
        Index index;
        if (referencing) {
            rowColumns = fkey.getReferencingColumns();
            indexColumns = fkey.getReferencedColumns();
            index = fkey.getReferencedIndex();
        }
        else {
            rowColumns = fkey.getReferencedColumns();
            indexColumns = fkey.getReferencingColumns();
            index = fkey.getReferencingIndex();
        }
        int ncols = rowColumns.size();
        if (ncols <= 1) {
            return rowColumns;
        }
        List<Column> result = new ArrayList<>(ncols);
        for (int i = 0; i < ncols; i++) {
            Column keyColumn = index.getKeyColumns().get(i).getColumn();
            result.add(rowColumns.get(indexColumns.indexOf(keyColumn)));
        }
        return result;
    }

    protected class ForeignKeyHandler implements Handler {
        protected final ForeignKey foreignKey;
        protected final boolean referencing, referenced;
        // referencingColumns in order of referencedIndex.
        protected final List<Column> crossReferencingColumns;
        // referencedColumns in order of referencingIndex.
        protected final List<Column> crossReferencedColumns;
        protected UpdatePlannable updatePlan, deletePlan, truncatePlan;

        public ForeignKeyHandler(ForeignKey foreignKey, Table forTable) {
            this.foreignKey = foreignKey;
            this.referencing = (foreignKey.getReferencingTable() == forTable);
            this.referenced = (foreignKey.getReferencedTable() == forTable);
            this.crossReferencingColumns = (referencing) ? crossReferenceColumns(foreignKey, true) : null;
            this.crossReferencedColumns = (referenced) ? crossReferenceColumns(foreignKey, false) : null;
        }

        public void handleInsert(Session session, RowData row) {
            if (referencing) {
                checkReferencing(session, row, foreignKey, crossReferencingColumns, "insert into");
            }
        }

        public void handleUpdate(Session session, RowData oldRow, RowData newRow) {
            if (referencing &&
                anyColumnChanged(session, oldRow, newRow,
                                 foreignKey.getReferencingColumns())) {
                checkReferencing(session, newRow, foreignKey, crossReferencingColumns, "update");
            }
            if (referenced &&
                anyColumnChanged(session, oldRow, newRow,
                                 foreignKey.getReferencedColumns())) {
                switch (foreignKey.getUpdateAction()) {
                case RESTRICT:
                    checkNotReferenced(session, oldRow, foreignKey, crossReferencedColumns, "update");
                default:
                    runOperatorPlan(getUpdatePlan(), session, crossReferencedColumns,
                                    oldRow, newRow);
                }
            }
        }

        public void handleDelete(Session session, RowData row) {
            if (referenced) {
                switch (foreignKey.getUpdateAction()) {
                case RESTRICT:
                    checkNotReferenced(session, row, foreignKey, crossReferencedColumns, "delete from");
                default:
                    runOperatorPlan(getDeletePlan(), session, crossReferencedColumns,
                                    row, null);
                }
            }
        }

        public void handleTruncate(Session session) {
            if (referenced) {
                if (referencing) {
                    // Self-join no problem when whole table truncated.
                    return;
                }
                switch (foreignKey.getUpdateAction()) {
                case RESTRICT:
                    checkNotReferenced(session, null, foreignKey, crossReferencedColumns, "truncate");
                default:
                    runOperatorPlan(getTruncatePlan(), session, crossReferencedColumns,
                                    null, null);
                }
            }
        }

        protected synchronized UpdatePlannable getUpdatePlan() {
            if (updatePlan == null) {
                updatePlan = buildPlan(foreignKey, true, true);
            }
            return updatePlan;
        }

        protected synchronized UpdatePlannable getDeletePlan() {
            if (deletePlan == null) {
                deletePlan = buildPlan(foreignKey, true, false);
            }
            return deletePlan;
        }

        protected synchronized UpdatePlannable getTruncatePlan() {
            if (truncatePlan == null) {
                truncatePlan = buildPlan(foreignKey, false, false);
            }
            return truncatePlan;
        }

    }

    protected boolean anyColumnChanged(Session session, RowData oldRow, RowData newRow,
                                       List<Column> columns) {
        RowDef rowDef = columns.get(0).getTable().rowDef();
        for (Column column : columns) {
            if (!AbstractStore.fieldEqual(rowDef, oldRow, rowDef, newRow,
                                          column.getPosition())) {
                return true;
            }
        }
        return false;
    }

    protected void checkReferencing(Session session, RowData row, 
                                    ForeignKey foreignKey, List<Column> columns,
                                    String action) {
        Index index = foreignKey.getReferencedIndex();
        SDType storeData = (SDType)store.createStoreData(session, index);
        Key key = store.getKey(session, storeData);
        try {
            boolean anyNull = crossReferenceKey(session, key, row, columns);
            if (!anyNull) {
                assert index.isUnique();
                if (index.isUniqueAndMayContainNulls()) {
                    key.append(0L);
                }
                checkReferencing(session, index, storeData, row, foreignKey, action);
            }
        }
        finally {
            store.releaseStoreData(session, storeData);
        }
    }

    protected abstract void checkReferencing(Session session, Index index, SDType storeData,
                                             RowData row, ForeignKey foreignKey, String action);

    protected void notReferencing(Session session, Index index, SDType storeData,
                                  RowData row, ForeignKey foreignKey, String action) {
        String key = formatKey(session, row, foreignKey.getReferencingColumns());
        throw new ForeignKeyReferencingViolationException(action,
                                                          foreignKey.getReferencingTable().getName(),
                                                          key,
                                                          foreignKey.getConstraintName(),
                                                          foreignKey.getReferencedTable().getName());
    }

    protected void checkNotReferenced(Session session, RowData row, 
                                      ForeignKey foreignKey, List<Column> columns,
                                      String action) {
        Index index = foreignKey.getReferencingIndex();
        SDType storeData = (SDType)store.createStoreData(session, index);
        Key key = store.getKey(session, storeData);
        try {
            boolean anyNull = crossReferenceKey(session, key, row, columns);
            if (!anyNull) {
                checkNotReferenced(session, index, storeData, row, foreignKey, action);
            }
        }
        finally {
            store.releaseStoreData(session, storeData);
        }
    }

    protected abstract void checkNotReferenced(Session session, Index index, SDType storeData,
                                               RowData row, ForeignKey foreignKey, String action);
    
    protected void stillReferenced(Session session, Index index, SDType storeData,
                                   RowData row, ForeignKey foreignKey, String action) {
        String key;
        if (row == null) {
            Key foundKey = store.getKey(session, storeData);
            StringBuilder str = new StringBuilder();
            // Truncate: check for nulls, which are okay.
            foundKey.reset();
            for (int i = 0; i < index.getKeyColumns().size(); i++) {
                if (i > 0) {
                    str.append(" and ");
                }
                str.append(foreignKey.getReferencedColumns().get(foreignKey.getReferencingColumns().indexOf(index.getKeyColumns().get(i).getColumn())).getName());
                str.append(" = ");
                if (foundKey.isNull()) {
                    return;
                }
                else {
                    str.append(foundKey.decode());
                }
            }
            key = str.toString();
        }
        else {
            key = formatKey(session, row, foreignKey.getReferencedColumns());
        }
        throw new ForeignKeyReferencedViolationException(action,
                                                         foreignKey.getReferencedTable().getName(),

                                                         key,
                                                         foreignKey.getConstraintName(),
                                                         foreignKey.getReferencingTable().getName());
    }

    protected static boolean crossReferenceKey(Session session, Key key,
                                               RowData row, List<Column> columns) {
        key.clear();
        if (row == null) {
            // This is the truncate case, find all non-null referencing index entries.
            key.append(null);
            return false;
        }
        RowDataValueSource source = new RowDataValueSource();
        PersistitKeyValueTarget target = new PersistitKeyValueTarget();
        target.attach(key);
        boolean anyNull = false;
        for (Column column : columns) {
            source.bind(column.getFieldDef(), row);
            if (source.isNull()) {
                target.putNull();
                anyNull = true;
            }
            else {
                source.tInstance().writeCollating(source, target);
            }
        }
        return anyNull;
    }

    protected String formatKey(Session session, RowData row, List<Column> columns) {
        RowDataValueSource source = new RowDataValueSource();
        StringBuilder str = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(str);
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                str.append(" and ");
            }
            Column column = columns.get(i);
            str.append(column.getName()).append(" = ");
            source.bind(column.getFieldDef(), row);
            source.tInstance().format(source, appender);
        }
        return str.toString();
    }

    protected UpdatePlannable buildPlan(ForeignKey foreignKey,
                                        boolean hasOldRow, boolean hasNewRow) {
        return null;
    }

    protected void runOperatorPlan(UpdatePlannable plan, Session session,
                                   List<Column> referencedColumns,
                                   RowData oldRow, RowData newRow) {
    }

}
