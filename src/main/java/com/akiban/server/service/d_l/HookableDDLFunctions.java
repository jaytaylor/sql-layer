/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service.d_l;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowDef;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.GenericInvalidOperationException;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.ddl.DuplicateColumnNameException;
import com.akiban.server.api.ddl.DuplicateTableNameException;
import com.akiban.server.api.ddl.ForeignConstraintDDLException;
import com.akiban.server.api.ddl.GroupWithProtectedTableException;
import com.akiban.server.api.ddl.JoinToMultipleParentsException;
import com.akiban.server.api.ddl.JoinToUnknownTableException;
import com.akiban.server.api.ddl.JoinToWrongColumnsException;
import com.akiban.server.api.ddl.NoPrimaryKeyException;
import com.akiban.server.api.ddl.ParseException;
import com.akiban.server.api.ddl.ProtectedTableDDLException;
import com.akiban.server.api.ddl.UnsupportedCharsetException;
import com.akiban.server.api.ddl.UnsupportedDataTypeException;
import com.akiban.server.api.ddl.UnsupportedDropException;
import com.akiban.server.api.ddl.UnsupportedIndexDataTypeException;
import com.akiban.server.service.d_l.DStarLFunctionsHook.DDLFunction;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import com.akiban.server.store.SchemaId;

import java.util.Collection;
import java.util.List;

import static com.akiban.server.service.d_l.HookUtil.launder;

public final class HookableDDLFunctions implements DDLFunctions {

    private final DDLFunctions delegate;
    private final DStarLFunctionsHook hook;

    public HookableDDLFunctions(DDLFunctions delegate, List<DStarLFunctionsHook> hooks) {
        this.delegate = delegate;
        this.hook = hooks.size() == 1 ? hooks.get(0) : new CompositeHook(hooks);
    }

    @Override
    public void createTable(Session session, String schema, String ddlText) throws ParseException, UnsupportedCharsetException, ProtectedTableDDLException, DuplicateTableNameException, GroupWithProtectedTableException, JoinToUnknownTableException, JoinToWrongColumnsException, JoinToMultipleParentsException, NoPrimaryKeyException, DuplicateColumnNameException, UnsupportedDataTypeException, UnsupportedIndexDataTypeException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.CREATE_TABLE);
            delegate.createTable(session, schema, ddlText);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.CREATE_TABLE, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.CREATE_TABLE);
        }
    }

    @Override
    public void dropTable(Session session, TableName tableName) throws ProtectedTableDDLException, ForeignConstraintDDLException, UnsupportedDropException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.DROP_TABLE);
            delegate.dropTable(session, tableName);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.DROP_TABLE, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.DROP_TABLE);
        }
    }

    @Override
    public void dropSchema(Session session, String schemaName) throws ProtectedTableDDLException, ForeignConstraintDDLException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.DROP_SCHEMA);
            delegate.dropSchema(session, schemaName);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.DROP_SCHEMA, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.DROP_SCHEMA);
        }
    }

    @Override
    public void dropGroup(Session session, String groupName) throws ProtectedTableDDLException, GenericInvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.DROP_GROUP);
            delegate.dropGroup(session, groupName);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.DROP_GROUP, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.DROP_GROUP);
        }
    }

    @Override
    public AkibanInformationSchema getAIS(final Session session) {
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_AIS);
            return delegate.getAIS(session);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_AIS, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_AIS);
        }
    }

    @Override
    public int getTableId(Session session, TableName tableName) throws NoSuchTableException {
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_TABLE_ID);
            return delegate.getTableId(session, tableName);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_TABLE_ID, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_TABLE_ID);
        }
    }

    @Override
    public Table getTable(Session session, int tableId) throws NoSuchTableException {
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_TABLE_BY_ID);
            return delegate.getTable(session, tableId);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_TABLE_BY_ID, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_TABLE_BY_ID);
        }
    }

    @Override
    public Table getTable(Session session, TableName tableName) throws NoSuchTableException {
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_TABLE_BY_NAME);
            return delegate.getTable(session, tableName);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_TABLE_BY_NAME, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_TABLE_BY_NAME);
        }
    }

    @Override
    public UserTable getUserTable(Session session, TableName tableName) throws NoSuchTableException {
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_USER_TABLE_BY_NAME);
            return delegate.getUserTable(session, tableName);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_USER_TABLE_BY_NAME, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_USER_TABLE_BY_NAME);
        }
    }

    @Override
    public TableName getTableName(Session session, int tableId) throws NoSuchTableException {
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_USER_TABLE_BY_ID);
            return delegate.getTableName(session, tableId);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_USER_TABLE_BY_ID, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_USER_TABLE_BY_ID);
        }
    }

    @Override
    public RowDef getRowDef(int tableId) throws NoSuchTableException {
        Session session = new SessionImpl();
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_ROWDEF);
            return delegate.getRowDef(tableId);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_ROWDEF, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_ROWDEF);
        }
    }

    @Override
    public String getDDLs(final Session session) throws InvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_DDLS);
            return delegate.getDDLs(session);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_DDLS, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_DDLS);
        }
    }

    @Override
    public SchemaId getSchemaID() throws InvalidOperationException {
        Session session = new SessionImpl();
        try {
            hook.hookFunctionIn(session, DDLFunction.GET_SCHEMA_ID);
            return delegate.getSchemaID();
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.GET_SCHEMA_ID, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.GET_SCHEMA_ID);
        }
    }

    @Override
    public void forceGenerationUpdate() throws InvalidOperationException {
        Session session = new SessionImpl();
        try {
            hook.hookFunctionIn(session, DDLFunction.FORCE_GENERATION_UPDATE);
            delegate.forceGenerationUpdate();
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.FORCE_GENERATION_UPDATE, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.FORCE_GENERATION_UPDATE);
        }
    }

    @Override
    public void createIndexes(final Session session, Collection<Index> indexesToAdd) throws InvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.CREATE_INDEXES);
            delegate.createIndexes(session, indexesToAdd);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.CREATE_INDEXES, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.CREATE_INDEXES);
        }
    }

    @Override
    public void dropIndexes(final Session session, TableName tableName, Collection<String> indexNamesToDrop) throws InvalidOperationException {
        try {
            hook.hookFunctionIn(session, DDLFunction.DROP_INDEXES);
            delegate.dropIndexes(session, tableName, indexNamesToDrop);
        } catch (Throwable t) {
            hook.hookFunctionCatch(session, DDLFunction.DROP_INDEXES, t);
            throw launder(t);
        } finally {
            hook.hookFunctionFinally(session, DDLFunction.DROP_INDEXES);
        }
    }
}
