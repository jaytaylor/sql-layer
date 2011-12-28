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
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;

import java.util.Collection;
import java.util.List;

import static com.akiban.util.Exceptions.throwAlways;
import static com.akiban.util.Exceptions.throwIfInstanceOf;

public final class HookableDDLFunctions implements DDLFunctions {

    private final DDLFunctions delegate;
    private final DXLFunctionsHook hook;
    private final SessionService sessionService;

    public HookableDDLFunctions(DDLFunctions delegate, List<DXLFunctionsHook> hooks, SessionService sessionService) {
        this.delegate = delegate;
        this.sessionService = sessionService;
        this.hook = hooks.size() == 1 ? hooks.get(0) : new CompositeHook(hooks);
    }
    
    @Override
    public void createTable(Session session, UserTable table) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CREATE_TABLE);
            delegate.createTable(session, table);
        }catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.CREATE_TABLE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CREATE_TABLE, thrown);
        }
    }

    @Override
    public void createTable(Session session, String schema, String ddlText) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CREATE_TABLE);
            delegate.createTable(session, schema, ddlText);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.CREATE_TABLE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CREATE_TABLE, thrown);
        }
    }


    @Override
    public void renameTable(Session session, TableName currentName, TableName newName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.RENAME_TABLE);
            delegate.renameTable(session, currentName, newName);
        }catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.RENAME_TABLE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.RENAME_TABLE, thrown);
        }
    }

    @Override
    public void dropTable(Session session, TableName tableName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_TABLE);
            delegate.dropTable(session, tableName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_TABLE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_TABLE, thrown);
        }
    }

    @Override
    public void dropSchema(Session session, String schemaName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_SCHEMA);
            delegate.dropSchema(session, schemaName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_SCHEMA, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_SCHEMA, thrown);
        }
    }

    @Override
    public void dropGroup(Session session, String groupName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_GROUP);
            delegate.dropGroup(session, groupName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_GROUP, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_GROUP, thrown);
        }
    }

    @Override
    public AkibanInformationSchema getAIS(final Session session) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_AIS);
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
    public int getTableId(Session session, TableName tableName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_TABLE_ID);
            return delegate.getTableId(session, tableName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_TABLE_ID, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_TABLE_ID, thrown);
        }
    }

    @Override
    public Table getTable(Session session, int tableId) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_TABLE_BY_ID);
            return delegate.getTable(session, tableId);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_TABLE_BY_ID, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_TABLE_BY_ID, thrown);
        }
    }

    @Override
    public Table getTable(Session session, TableName tableName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_TABLE_BY_NAME);
            return delegate.getTable(session, tableName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_TABLE_BY_NAME, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_TABLE_BY_NAME, thrown);
        }
    }

    @Override
    public UserTable getUserTable(Session session, TableName tableName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_USER_TABLE_BY_NAME);
            return delegate.getUserTable(session, tableName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_USER_TABLE_BY_NAME, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_USER_TABLE_BY_NAME, thrown);
        }
    }

    @Override
    public TableName getTableName(Session session, int tableId) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_USER_TABLE_BY_ID);
            return delegate.getTableName(session, tableId);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_USER_TABLE_BY_ID, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_USER_TABLE_BY_ID, thrown);
        }
    }

    @Override
    public RowDef getRowDef(int tableId) {
        Session session = sessionService.createSession();
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_ROWDEF);
            return delegate.getRowDef(tableId);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_ROWDEF, t);
            throw throwAlways(t);
        } finally {
            try {
                hook.hookFunctionFinally(session, DXLFunction.GET_ROWDEF, thrown);
            } finally {
                session.close();
            }
        }
    }

    @Override
    public List<String> getDDLs(final Session session) throws InvalidOperationException {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_DDLS);
            return delegate.getDDLs(session);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_DDLS, t);
            throwIfInstanceOf(t, InvalidOperationException.class);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_DDLS, thrown);
        }
    }

    @Override
    public int getGeneration() {
        Session session = sessionService.createSession();
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_SCHEMA_ID);
            return delegate.getGeneration();
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_SCHEMA_ID, t);
            throw throwAlways(t);
        } finally {
            try {
                hook.hookFunctionFinally(session, DXLFunction.GET_SCHEMA_ID, thrown);
            } finally {
                session.close();
            }
        }
    }

    @Override
    public long getTimestamp() {
        Session session = sessionService.createSession();
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_SCHEMA_TIMESTAMP);
            return delegate.getTimestamp();
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_SCHEMA_TIMESTAMP, t);
            throw throwAlways(t);
        } finally {
            try {
                hook.hookFunctionFinally(session, DXLFunction.GET_SCHEMA_TIMESTAMP, thrown);
            } finally {
                session.close();
            }
        }
    }

    @Override
    public void forceGenerationUpdate() {
        Session session = sessionService.createSession();
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.FORCE_GENERATION_UPDATE);
            delegate.forceGenerationUpdate();
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.FORCE_GENERATION_UPDATE, t);
            throw throwAlways(t);
        } finally {
            try {
                hook.hookFunctionFinally(session, DXLFunction.FORCE_GENERATION_UPDATE, thrown);
            } finally {
                session.close();
            }
        }
    }

    @Override
    public void createIndexes(final Session session, Collection<? extends Index> indexesToAdd) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CREATE_INDEXES);
            delegate.createIndexes(session, indexesToAdd);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.CREATE_INDEXES, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CREATE_INDEXES, thrown);
        }
    }

    @Override
    public void dropTableIndexes(final Session session, TableName tableName, Collection<String> indexNamesToDrop) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_INDEXES);
            delegate.dropTableIndexes(session, tableName, indexNamesToDrop);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_INDEXES, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_INDEXES, thrown);
        }
    }

    @Override
    public void dropGroupIndexes(Session session, String groupName, Collection<String> indexesToDrop) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_INDEXES);
            delegate.dropGroupIndexes(session, groupName, indexesToDrop);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_INDEXES, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_INDEXES, thrown);
        }
    }

    @Override
    public void updateTableStatistics(Session session, TableName tableName, Collection<String> indexesToUpdate) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.UPDATE_TABLE_STATISTICS);
            delegate.updateTableStatistics(session, tableName, indexesToUpdate);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.UPDATE_TABLE_STATISTICS, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.UPDATE_TABLE_STATISTICS, thrown);
        }
    }

}
