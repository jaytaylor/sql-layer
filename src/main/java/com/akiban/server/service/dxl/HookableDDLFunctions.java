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

package com.akiban.server.service.dxl;

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
import com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;

import java.util.Collection;
import java.util.List;

import static com.akiban.server.service.dxl.HookUtil.throwIf;
import static com.akiban.util.Exceptions.throwAlways;

public final class HookableDDLFunctions implements DDLFunctions {

    private final DDLFunctions delegate;
    private final DXLFunctionsHook hook;

    public HookableDDLFunctions(DDLFunctions delegate, List<DXLFunctionsHook> hooks) {
        this.delegate = delegate;
        this.hook = hooks.size() == 1 ? hooks.get(0) : new CompositeHook(hooks);
    }

    @Override
    public void createTable(Session session, String schema, String ddlText) throws ParseException, UnsupportedCharsetException, ProtectedTableDDLException, DuplicateTableNameException, GroupWithProtectedTableException, JoinToUnknownTableException, JoinToWrongColumnsException, JoinToMultipleParentsException, NoPrimaryKeyException, DuplicateColumnNameException, UnsupportedDataTypeException, UnsupportedIndexDataTypeException, GenericInvalidOperationException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CREATE_TABLE);
            delegate.createTable(session, schema, ddlText);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.CREATE_TABLE, t);
            throwIf(t, ParseException.class);
            throwIf(t, UnsupportedCharsetException.class);
            throwIf(t, ProtectedTableDDLException.class);
            throwIf(t, DuplicateTableNameException.class);
            throwIf(t, GroupWithProtectedTableException.class);
            throwIf(t, JoinToUnknownTableException.class);
            throwIf(t, JoinToWrongColumnsException.class);
            throwIf(t, JoinToMultipleParentsException.class);
            throwIf(t, NoPrimaryKeyException.class);
            throwIf(t, DuplicateColumnNameException.class);
            throwIf(t, UnsupportedDataTypeException.class);
            throwIf(t, UnsupportedIndexDataTypeException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CREATE_TABLE, thrown);
        }
    }

    @Override
    public void dropTable(Session session, TableName tableName) throws ProtectedTableDDLException, ForeignConstraintDDLException, UnsupportedDropException, GenericInvalidOperationException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_TABLE);
            delegate.dropTable(session, tableName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_TABLE, t);
            throwIf(t, ProtectedTableDDLException.class);
            throwIf(t, ForeignConstraintDDLException.class);
            throwIf(t, UnsupportedDropException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_TABLE, thrown);
        }
    }

    @Override
    public void dropSchema(Session session, String schemaName) throws ProtectedTableDDLException, ForeignConstraintDDLException, GenericInvalidOperationException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_SCHEMA);
            delegate.dropSchema(session, schemaName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_SCHEMA, t);
            throwIf(t, ProtectedTableDDLException.class);
            throwIf(t, ForeignConstraintDDLException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_SCHEMA, thrown);
        }
    }

    @Override
    public void dropGroup(Session session, String groupName) throws ProtectedTableDDLException, GenericInvalidOperationException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_GROUP);
            delegate.dropGroup(session, groupName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.DROP_GROUP, t);
            throwIf(t, ProtectedTableDDLException.class);
            throwIf(t, GenericInvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_GROUP, thrown);
        }
    }

    @Override
    public AkibanInformationSchema getAIS(final Session session) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_AIS);
            return delegate.getAIS(session);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_AIS, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_AIS, thrown);
        }
    }

    @Override
    public int getTableId(Session session, TableName tableName) throws NoSuchTableException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_TABLE_ID);
            return delegate.getTableId(session, tableName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_TABLE_ID, t);
            throwIf(t, NoSuchTableException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.GET_TABLE_ID, thrown);
        }
    }

    @Override
    public Table getTable(Session session, int tableId) throws NoSuchTableException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_TABLE_BY_ID);
            return delegate.getTable(session, tableId);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_TABLE_BY_ID, t);
            throwIf(t, NoSuchTableException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_TABLE_BY_ID, thrown);
        }
    }

    @Override
    public Table getTable(Session session, TableName tableName) throws NoSuchTableException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_TABLE_BY_NAME);
            return delegate.getTable(session, tableName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_TABLE_BY_NAME, t);
            throwIf(t, NoSuchTableException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_TABLE_BY_NAME, thrown);
        }
    }

    @Override
    public UserTable getUserTable(Session session, TableName tableName) throws NoSuchTableException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_USER_TABLE_BY_NAME);
            return delegate.getUserTable(session, tableName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_USER_TABLE_BY_NAME, t);
            throwIf(t, NoSuchTableException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_USER_TABLE_BY_NAME, thrown);
        }
    }

    @Override
    public TableName getTableName(Session session, int tableId) throws NoSuchTableException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_USER_TABLE_BY_ID);
            return delegate.getTableName(session, tableId);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_USER_TABLE_BY_ID, t);
            throwIf(t, NoSuchTableException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_USER_TABLE_BY_ID, thrown);
        }
    }

    @Override
    public RowDef getRowDef(int tableId) throws NoSuchTableException {
        Session session = new SessionImpl();
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_ROWDEF);
            return delegate.getRowDef(tableId);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_ROWDEF, t);
            throwIf(t, NoSuchTableException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_ROWDEF, thrown);
        }
    }

    @Override
    public String getDDLs(final Session session) throws InvalidOperationException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_DDLS);
            return delegate.getDDLs(session);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_DDLS, t);
            throwIf(t, InvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.GET_DDLS, thrown);
        }
    }

    @Override
    public int getGeneration() {
        Session session = new SessionImpl();
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_SCHEMA_ID);
            return delegate.getGeneration();
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_SCHEMA_ID, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_SCHEMA_ID, thrown);
        }
    }

    @Override
    public void forceGenerationUpdate() {
        Session session = new SessionImpl();
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.FORCE_GENERATION_UPDATE);
            delegate.forceGenerationUpdate();
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.FORCE_GENERATION_UPDATE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.FORCE_GENERATION_UPDATE, thrown);
        }
    }

    @Override
    public void createIndexes(final Session session, Collection<Index> indexesToAdd) throws InvalidOperationException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.CREATE_INDEXES);
            delegate.createIndexes(session, indexesToAdd);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.CREATE_INDEXES, t);
            throwIf(t, InvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.CREATE_INDEXES, thrown);
        }
    }

    @Override
    public void dropIndexes(final Session session, TableName tableName, Collection<String> indexNamesToDrop) throws InvalidOperationException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.DROP_INDEXES);
            delegate.dropIndexes(session, tableName, indexNamesToDrop);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_INDEXES, t);
            throwIf(t, InvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_INDEXES, thrown);
        }
    }
}
