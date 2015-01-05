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
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.expression.RowBasedUnboundExpressions;
import com.foundationdb.qp.expression.UnboundExpressions;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.PersistitKeyValueTarget;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.error.ForeignKeyReferencedViolationException;
import com.foundationdb.server.error.ForeignKeyReferencingViolationException;
import com.foundationdb.server.error.NotNullViolationException;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Explainer;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.config.PropertyNotDefinedException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.texpressions.TPreparedFunction;
import com.foundationdb.server.types.texpressions.TPreparedParameter;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.Strings;
import com.persistit.Key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
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
    private static final Logger LOG = LoggerFactory.getLogger(ConstraintHandler.class);

    protected final SType store;
    protected final int groupLookupPipelineQuantum;
    protected final TypesRegistryService typesRegistryService;
    protected final ServiceManager serviceManager;

    protected ConstraintHandler(SType store, ConfigurationService config, TypesRegistryService typesRegistryService, ServiceManager serviceManager) {
        this.store = store;
        int quantum;
        try {
            quantum = Integer.parseInt(config.getProperty("fdbsql.pipeline.groupLookup.lookaheadQuantum"));
        }
        catch (PropertyNotDefinedException ex) {
            quantum = 1;
        }
        this.groupLookupPipelineQuantum = quantum;
        this.typesRegistryService = typesRegistryService;
        this.serviceManager = serviceManager;
    }

    public void handleInsert(Session session, Table table, Row row) {
        Handler th = getTableHandler(table);
        if (th != null) {
            th.handleInsert(session, row);
        }
    }
    
    public void handleUpdatePre(Session session, Table table,
                                Row oldRow, Row newRow) {
        Handler th = getTableHandler(table);
        if (th != null) {
            th.handleUpdatePre(session, oldRow, newRow);
        }
    }
    
    public void handleUpdatePost(Session session, Table table,
                                Row oldRow, Row newRow) {
        Handler th = getTableHandler(table);
        if (th != null) {
            th.handleUpdatePost(session, oldRow, newRow);
        }
    }

    public void handleDelete(Session session, Table table, Row row) {
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
        ArrayList<Handler> handlers = new ArrayList<>(fkeys.size() + 1);
        if (!table.notNull().isEmpty()) {
            handlers.add(new NotNullHandler(table));
        }
        for (ForeignKey fkey : fkeys) {
            handlers.add(new ForeignKeyHandler(fkey, table));
        }
        switch (handlers.size()) {
            case 0:
                return null;
            case 1:
                return handlers.get(0);
            default:
                handlers.trimToSize();
                return new CompoundHandler(handlers);
        }
    }

    protected interface Handler {
        public void handleInsert(Session session, Row row);
        public void handleUpdatePre(Session session, Row oldRow, Row newRow);
        public void handleUpdatePost(Session session, Row oldRow, Row newRow);
        public void handleDelete(Session session, Row row);
        public void handleTruncate(Session session);
    }

    protected class CompoundHandler implements Handler {
        protected final Collection<Handler> handlers;

        public CompoundHandler(Collection<Handler> handlers) {
            this.handlers = handlers;
        }

        @Override
        public void handleInsert (Session session, Row row) {
            for (Handler handler : handlers) {
                handler.handleInsert(session, row);
            }
        }
        
        @Override
        public void handleUpdatePre(Session session, Row oldRow, Row newRow) {
            for (Handler handler : handlers) {
                handler.handleUpdatePre(session, oldRow, newRow);
            }
        }
        
        @Override
        public void handleUpdatePost(Session session, Row oldRow, Row newRow) {
            for (Handler handler : handlers) {
                handler.handleUpdatePost(session, oldRow, newRow);
            }
        }
        
        @Override
        public void handleDelete(Session session, Row row){
            for (Handler handler : handlers) {
                handler.handleDelete(session, row);
            }
        }

        @Override
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

    protected static class NotNullHandler implements Handler {
        private final Table table;
        private final BitSet notNull;

        public NotNullHandler(Table table) {
            this.table = table;
            this.notNull = table.notNull();
        }

        @Override 
        public void handleInsert(Session session, Row row) {
            checkNotNull(row);
        }
        
        @Override
        public void handleUpdatePre(Session session, Row oldRow, Row newRow) {
            checkNotNull(newRow);
        }
        
        @Override
        public void handleUpdatePost(Session session, Row oldRow, Row newRow) {
            // Checked in pre
        }
        
        @Override
        public void handleDelete(Session session, Row row) {
            //None
        }
        
        @Override
        public void handleTruncate(Session session) {
            // None
        }

        private void checkNotNull(Row row) {
            for (int f = notNull.nextSetBit(0); f >= 0; f = notNull.nextSetBit(f+1)) {
                if (row.value(f).isNull()) {
                    throw new NotNullViolationException(table.getName().getSchemaName(),
                            table.getName().getTableName(),
                            table.getColumn(f).getName());
                }
            }
        }
    }

    protected class ForeignKeyHandler implements Handler {
        protected final ForeignKey foreignKey;
        protected final boolean referencing, referenced;
        // referencingColumns in order of referencedIndex.
        protected final List<Column> crossReferencingColumns;
        // referencedColumns in order of referencingIndex.
        protected final List<Column> crossReferencedColumns;
        protected Plan updatePlan, deletePlan, truncatePlan;

        public ForeignKeyHandler(ForeignKey foreignKey, Table forTable) {
            this.foreignKey = foreignKey;
            this.referencing = (foreignKey.getReferencingTable() == forTable);
            this.referenced = (foreignKey.getReferencedTable() == forTable);
            this.crossReferencingColumns = (referencing) ? crossReferenceColumns(foreignKey, true) : null;
            this.crossReferencedColumns = (referenced) ? crossReferenceColumns(foreignKey, false) : null;
        }

        @Override
        public void handleInsert(Session session, Row row) {
            if (referencing) {
                checkReferencing(session, row, foreignKey, crossReferencingColumns,
                        "insert into");
            }
        }
        
        @Override 
        public void handleUpdatePre(Session session, Row oldRow, Row newRow){
            if (referencing &&
                    anyColumnChanged(session, oldRow, newRow,
                                     foreignKey.getReferencingColumns())) {
                    checkReferencing(session, newRow, foreignKey, crossReferencingColumns,
                                     "update");
            }
            if (referenced &&
                anyColumnChanged(session, oldRow, newRow,
                                 foreignKey.getReferencedColumns())) {
                switch (foreignKey.getUpdateAction()) {
                case NO_ACTION:
                case RESTRICT:
                    checkNotReferenced(session, oldRow, foreignKey, crossReferencedColumns,
                                       foreignKey.getUpdateAction(), "update");
                    break;
                case CASCADE:
                    // This needs to refer to the after image of the row, so it needs
                    // to be done in handleUpdatePost
                    break;
                default:
                    runOperatorPlan(getUpdatePlan(), session, oldRow, newRow);
                }
            }
        }

        @Override
        public void handleUpdatePost (Session session, Row oldRow, Row newRow) {
            if(referenced &&
                    (foreignKey.getUpdateAction() == ForeignKey.Action.CASCADE) &&
                    anyColumnChanged(session, oldRow, newRow, foreignKey.getReferencedColumns())) {
                 runOperatorPlan(getUpdatePlan(), session, oldRow, newRow);
             }
        }
        
        @Override
        public void handleDelete(Session session, Row row) {
            if (referenced) {
                switch (foreignKey.getDeleteAction()) {
                case NO_ACTION:
                case RESTRICT:
                    checkNotReferenced(session, row, foreignKey, crossReferencedColumns,
                                       foreignKey.getDeleteAction(), "delete from");
                    break;
                default:
                    runOperatorPlan(getDeletePlan(), session, row, null);
                }
            }
        }
        
        @Override 
        public void handleTruncate(Session session) {
            if (referenced) {
                if (referencing) {
                    // Self-join no problem when whole table truncated.
                    return;
                }
                switch (foreignKey.getDeleteAction()) {
                case NO_ACTION:
                case RESTRICT:
                    checkNotReferenced(session, (Row)null, foreignKey, crossReferencedColumns,
                                       foreignKey.getDeleteAction(), "truncate");
                    break;
                default:
                    runOperatorPlan(getTruncatePlan(), session, (Row)null, (Row)null);
                }
            }
        }

        protected synchronized Plan getUpdatePlan() {
            if (updatePlan == null) {
                updatePlan = buildPlan(foreignKey, crossReferencedColumns, true, true);
            }
            return updatePlan;
        }

        protected synchronized Plan getDeletePlan() {
            if (deletePlan == null) {
                deletePlan = buildPlan(foreignKey, crossReferencedColumns, true, false);
            }
            return deletePlan;
        }

        protected synchronized Plan getTruncatePlan() {
            if (truncatePlan == null) {
                truncatePlan = buildPlan(foreignKey, crossReferencedColumns, false, false);
            }
            return truncatePlan;
        }

    }

    protected boolean anyColumnChanged(Session session, Row oldRow, Row newRow, 
                                        List<Column> columns) {
        for (Column column: columns) {
            int i = column.getPosition().intValue();
            if (!TClass.areEqual(oldRow.value(i), newRow.value(i))) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    protected void checkReferencing (Session session, Row row, 
            ForeignKey foreignKey, List<Column> columns,
            String operation) {
        if (!compareSelfReference(row, foreignKey)) {
            Index index = foreignKey.getReferencedIndex();
            SDType storeData = (SDType)store.createStoreData(session, index);
            Key key = store.getKey(session, storeData);
            try {
                boolean anyNull = crossReferenceKey(session, key, row, columns);
                if (!anyNull) {
                    assert index.isUnique();
                    checkReferencing(session, index, storeData, row, foreignKey, operation);
                }
            }
            finally {
                store.releaseStoreData(session, storeData);
            }
        }
    }
    
    protected boolean compareSelfReference (Row row, ForeignKey foreignKey) {
        boolean selfReference = false;
        if (row == null) {
            return selfReference;
        }
        if (foreignKey.getReferencedTable() == foreignKey.getReferencingTable()) {
            selfReference = true;
            
            for (JoinColumn join : foreignKey.getJoinColumns()) {
                ValueSource parent = row.value(join.getParent().getColumn().getPosition().intValue());
                ValueSource child  = row.value(join.getChild().getColumn().getPosition().intValue());
                TInstance pInst = parent.getType();
                TInstance cInst = child.getType();
                
                TKeyComparable comparable = typesRegistryService.getKeyComparable(pInst.typeClass(), cInst.typeClass());
                int c = (comparable != null) ?
                    comparable.getComparison().compare(pInst, parent, cInst, child) :
                    TClass.compare(pInst, parent, cInst, child);
                selfReference &= (c == 0);
            }
        }
        return selfReference;
    }

    protected abstract void checkReferencing(Session session, Index index, SDType storeData,
                                             Row row, ForeignKey foreignKey, String operation);
    
   
    protected void notReferencing(Session session, Index index, SDType storeData,
            Row row, ForeignKey foreignKey, String operation) {
        String key = formatKey(session, row, foreignKey.getReferencingColumns());
        throw new ForeignKeyReferencingViolationException(operation,
                                                          foreignKey.getReferencingTable().getName(),
                                                          key,
                                                          foreignKey.getConstraintName().getTableName(),
                                                          foreignKey.getReferencedTable().getName());
    }
    
    @SuppressWarnings("unchecked")
    protected void checkNotReferenced(Session session, Row row,
                                    ForeignKey foreignKey, List<Column> columns,
                                    ForeignKey.Action action, String operation) {
        Index index = foreignKey.getReferencingIndex();
        SDType storeData = (SDType)store.createStoreData(session, index);
        Key key = store.getKey(session, storeData);
        try {
            boolean anyNull = crossReferenceKey(session, key, row, columns);
            if (!anyNull) {
                checkNotReferenced(session, index, storeData, row, foreignKey,
                                   compareSelfReference(row, foreignKey), action, operation);
            }
        }
        finally {
            store.releaseStoreData(session, storeData);
        }
    }
    
    protected abstract void checkNotReferenced(Session session, Index index, SDType storeData,
                                                Row row, ForeignKey foreignKey,
                                                boolean selfReference, ForeignKey.Action action, String operation);
    
    @SuppressWarnings("unchecked")
    protected void stillReferenced(Session session, Index index, SDType storeData,
                                    Row row, ForeignKey foreignKey, String operation) {
        String key;
        if (row == null) {
        Key foundKey = store.getKey(session, storeData);
        key = formatKey(session, index, foundKey, foreignKey.getReferencedColumns(), foreignKey.getReferencingColumns());
        }
        else {
        key = formatKey(session, row, foreignKey.getReferencedColumns());
        }
        throw new ForeignKeyReferencedViolationException(operation,
                                          foreignKey.getReferencedTable().getName(),
                                          key,
                                          foreignKey.getConstraintName().getTableName(),
                                          foreignKey.getReferencingTable().getName());
    }
    
    protected static boolean crossReferenceKey(Session session, Key key, Row row, List<Column> columns) {
        key.clear();
        if (row == null) {
            // This is the truncate case, find all non-null referencing index entries.
            key.append(null);
            return false;
        }
        boolean anyNull = false;

        PersistitKeyValueTarget target = new PersistitKeyValueTarget(ConstraintHandler.class.getSimpleName());
        target.attach(key);

        for (Column column : columns) {
            ValueSource source = row.value(column.getPosition().intValue());
            if (source.isNull()) {
                target.putNull();
                anyNull = true;
            } else {
                source.getType().writeCanonical(source, target);
            }
        }
        return anyNull;
    }

    public static boolean keyHasNullSegments(Key key, Index index) {
        key.reset();
        for (int i = 0; i < index.getKeyColumns().size(); i++) {
            if (key.isNull()) {
                return true;
            }
            else {
                key.decode();
            }
        }
        return false;
    }

    public static String formatKey(Session session, Row row, List<Column> columns) {
        StringBuilder str = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(str);
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                str.append(" and ");
            }
            Column column = columns.get(i);
            str.append(column.getName()).append(" = ");
            ValueSource source = row.value(column.getPosition());
            source.getType().format(source, appender);
        }
        return str.toString();
        
    }
    
    public static String formatKey(Session session, Index index, Key key,
                                   List<Column> reportColumns, List<Column> indexColumns) {
        StringBuilder str = new StringBuilder();
        key.reset();
        for (int i = 0; i < index.getKeyColumns().size(); i++) {
            if (i > 0) {
                str.append(" and ");
            }
            str.append(reportColumns.get(indexColumns.indexOf(index.getKeyColumns().get(i).getColumn())).getName());
            str.append(" = ");
            str.append(key.decode());
        }
        return str.toString();
    }
    
    protected static class Plan implements ColumnSelector, UpdateFunction {
        Schema schema;
        Operator operator;
        int ncols;
        int[] referencedColumns;
        boolean bindOldRow;
        boolean bindNewRow;
        ValueSource[] bindValues;
        int[] updatePositions;

        /* ColumnSelector */
        
        @Override
        public boolean includesColumn(int columnPosition) {
            return (columnPosition < ncols);
        }

        /* UpdateFunction */

        @Override
        public Row evaluate(Row original, QueryContext context, QueryBindings bindings) {
            OverlayingRow overlay = new OverlayingRow(original);
            for (int i = 0; i < ncols; i++) {
                overlay.overlay(updatePositions[i], 
                                // new values come after old keys.
                                bindings.getValue(ncols + i));
            }
            return overlay;
        }        
    }

    protected Plan buildPlan(ForeignKey foreignKey, List<Column> crossReferencedColumns,
                             boolean hasOldRow, boolean hasNewRow) {
        Plan plan = new Plan();
        plan.ncols = crossReferencedColumns.size();
        Group group = foreignKey.getReferencingTable().getGroup();
        plan.schema = SchemaCache.globalSchema(group.getAIS());
        TableRowType tableRowType = plan.schema.tableRowType(foreignKey.getReferencingTable());
        Operator input;
        if (hasOldRow) {
            // referencing WHERE fk = $1 AND...
            plan.bindOldRow = true;
            List<Column> referencedColumns = foreignKey.getReferencedColumns();
            plan.referencedColumns = new int[plan.ncols];
            for (int i = 0; i < plan.ncols; i++) {
                plan.referencedColumns[i] = referencedColumns.get(i).getPosition().intValue();
            }
            Index index = foreignKey.getReferencingIndex();
            IndexRowType indexRowType = plan.schema.indexRowType(index);
            List<TPreparedExpression> vars = new ArrayList<>(plan.ncols);
            for (int i = 0; i < plan.ncols; i++) {
                // Convert from index column position to parameter number.
                Column indexedColumn = crossReferencedColumns.get(i);
                int fkpos = referencedColumns.indexOf(indexedColumn);
                vars.add(new TPreparedParameter(fkpos, indexedColumn.getType()));
            }
            UnboundExpressions indexExprs = new RowBasedUnboundExpressions(indexRowType, vars);
            IndexBound indexBound = new IndexBound(indexExprs, plan);
            IndexKeyRange indexKeyRange = IndexKeyRange.bounded(indexRowType, indexBound, true, indexBound, true);
            input = API.indexScan_Default(indexRowType, indexKeyRange, 1);
            input = API.groupLookup_Default(input, group, indexRowType,
                                            Collections.singletonList(tableRowType),
                                            API.InputPreservationOption.DISCARD_INPUT,
                                            groupLookupPipelineQuantum);
        }
        else {
            // referencing WHERE fk IS NOT NULL AND...
            List<Column> referencingColumns = foreignKey.getReferencingColumns();
            TPreptimeValue emptyTPV = new TPreptimeValue();
            TValidatedScalar isNull = typesRegistryService.getScalarsResolver().get("IsNull", Collections.nCopies(1, emptyTPV)).getOverload();
            TValidatedScalar not = typesRegistryService.getScalarsResolver().get("NOT", Collections.nCopies(1, emptyTPV)).getOverload();
            TValidatedScalar and = typesRegistryService.getScalarsResolver().get("AND", Collections.nCopies(2, emptyTPV)).getOverload();
            TInstance boolType = AkBool.INSTANCE.instance(false);
            TPreparedExpression predicate = null;
            for (int i = 0; i < plan.ncols; i++) {
                Column referencingColumn = referencingColumns.get(i);
                TPreparedField field = new TPreparedField(referencingColumn.getType(), referencingColumn.getPosition());
                TPreparedExpression clause = new TPreparedFunction(isNull, boolType, Arrays.asList(field));
                clause = new TPreparedFunction(not, boolType, Arrays.asList(clause));
                if (predicate == null) {
                    predicate = clause;
                }
                else {
                    predicate = new TPreparedFunction(and, boolType, Arrays.asList(predicate, clause));
                }
            }
            input = API.groupScan_Default(group);
            input = API.filter_Default(input, Collections.singletonList(tableRowType));
            input = API.select_HKeyOrdered(input, tableRowType, predicate);
        }
        ForeignKey.Action action;
        takeAction: {
            if (hasNewRow) {
                action = foreignKey.getUpdateAction();
            }
            else {
                action = foreignKey.getDeleteAction();
                if (action == ForeignKey.Action.CASCADE) {
                    // DELETE FROM referencing ...
                    plan.operator = API.delete_Returning(input, false);
                    break takeAction;
                }
            }
            // UPDATE referencing SET fk = $2 ...
            switch (action) {
            case SET_NULL:
            case SET_DEFAULT:
                plan.bindValues = new ValueSource[plan.ncols];
                for (int i = 0; i < plan.ncols; i++) {
                    Column column = foreignKey.getReferencingColumns().get(i);
                    plan.bindValues[i] = ValueSources.fromObject((action == ForeignKey.Action.SET_NULL) ?
                                                                 null : 
                                                                 column.getDefaultValue(),
                                                                 column.getType()).value();
                }
                break;
            case CASCADE:
                plan.bindNewRow = true;
                break;
            default:
                assert false : action;
            }
            if (hasOldRow) {
                // Halloween vulnerability
                input = API.buffer_Default(input, tableRowType);
            }
            plan.updatePositions = new int[plan.ncols];
            for (int i = 0; i < plan.ncols; i++) {
                plan.updatePositions[i] = foreignKey.getReferencingColumns().get(i).getPosition();
            }
            plan.operator = API.update_Returning(input, plan);
        }
        if (LOG.isDebugEnabled()) {
            ExplainContext context = new ExplainContext();
            Attributes atts = new Attributes();
            TableName tableName = foreignKey.getReferencingTable().getName();
            atts.put(Label.TABLE_SCHEMA,
                     PrimitiveExplainer.getInstance(tableName.getSchemaName()));
            atts.put(Label.TABLE_NAME,
                     PrimitiveExplainer.getInstance(tableName.getTableName()));
            for (int i = 0; i < plan.ncols; i++) {
                atts.put(Label.COLUMN_NAME,
                         PrimitiveExplainer.getInstance(foreignKey.getReferencingColumns().get(i).getName()));
                CompoundExplainer var = new CompoundExplainer(Type.VARIABLE);
                var.addAttribute(Label.BINDING_POSITION,
                                 PrimitiveExplainer.getInstance(plan.ncols + i));
                atts.put(Label.EXPRESSIONS, var);
            }
            context.putExtraInfo(plan.operator,
                                 new CompoundExplainer(Type.EXTRA_INFO, atts));
            Explainer explainer = plan.operator.getExplainer(context);
            LOG.debug("Plan for " + foreignKey.getConstraintName().getTableName() + ":\n" +
                      Strings.join(new DefaultFormatter(tableName.getSchemaName()).format(explainer)));
        }
        return plan;
    }

    protected void runOperatorPlan (Plan plan, Session session, Row oldRow, Row newRow) {
        QueryContext context = 
                new SimpleQueryContext(store.createAdapter(session),
                                       serviceManager);
        QueryBindings bindings = context.createBindings();
        if (plan.bindOldRow) {
            for (int i = 0; i < plan.ncols; i++) {
                bindings.setValue(i, oldRow.value(plan.referencedColumns[i]));
            }
        }
        if (plan.bindNewRow) {
            for (int i = 0; i < plan.ncols; i++) {
                bindings.setValue(plan.referencedColumns.length + i, newRow.value(plan.referencedColumns[i]));
            }
        }
        else if (plan.bindValues != null) {
            for (int i = 0; i < plan.ncols; i++) {
                bindings.setValue(plan.bindValues.length + i, plan.bindValues[i]);
            }
        }
        Cursor cursor = API.cursor(plan.operator, context, bindings);
        cursor.openTopLevel();
        try {
            Row row;
            do {
                row = cursor.next();
            } while(row != null);
        } finally {
            cursor.closeTopLevel();
        }
    }
}
