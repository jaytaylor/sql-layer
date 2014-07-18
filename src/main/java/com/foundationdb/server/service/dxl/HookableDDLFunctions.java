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

package com.foundationdb.server.service.dxl;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.service.TypesRegistry;
import com.foundationdb.sql.server.ServerSession;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import static com.foundationdb.util.Exceptions.throwAlways;

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
    public void createTable(Session session, Table table) {
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
    public void createTable(Session session, Table table, String queryExpression, QueryContext context, ServerSession server) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CREATE_TABLE);
            delegate.createTable(session, table, queryExpression, context,  server);
        }catch (Throwable t) {
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
    public ChangeLevel alterTable(Session session, TableName tableName, Table newDefinition,
                                  List<TableChange> columnChanges, List<TableChange> indexChanges,
                                  QueryContext context) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.ALTER_TABLE);
            return delegate.alterTable(session, tableName, newDefinition, columnChanges, indexChanges, context);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.ALTER_TABLE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.ALTER_TABLE, thrown);
        }
    }

    @Override
    public void alterSequence(Session session, TableName sequenceName, Sequence newDefinition) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.ALTER_SEQUENCE);
            delegate.alterSequence(session, sequenceName, newDefinition);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.ALTER_SEQUENCE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.ALTER_SEQUENCE, thrown);
        }
    }

    @Override
    public void createView(Session session, View view) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CREATE_VIEW);
            delegate.createView(session, view);
        }catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.CREATE_VIEW, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CREATE_VIEW, thrown);
        }
    }

    @Override
    public void dropView(Session session, TableName viewName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_VIEW);
            delegate.dropView(session, viewName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_VIEW, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_VIEW, thrown);
        }
    }

    @Override
    public void createRoutine(Session session, Routine routine, boolean replaceExisting) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CREATE_ROUTINE);
            delegate.createRoutine(session, routine, replaceExisting);
        }catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.CREATE_ROUTINE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CREATE_ROUTINE, thrown);
        }
    }

    @Override
    public void dropRoutine(Session session, TableName routineName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_ROUTINE);
            delegate.dropRoutine(session, routineName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_ROUTINE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_ROUTINE, thrown);
        }
    }

    @Override
    public void createSQLJJar(Session session, SQLJJar sqljJar) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CREATE_SQLJ_JAR);
            delegate.createSQLJJar(session, sqljJar);
        }catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.CREATE_SQLJ_JAR, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CREATE_SQLJ_JAR, thrown);
        }
    }

    @Override
    public void replaceSQLJJar(Session session, SQLJJar sqljJar) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.REPLACE_SQLJ_JAR);
            delegate.replaceSQLJJar(session, sqljJar);
        }catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.REPLACE_SQLJ_JAR, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.REPLACE_SQLJ_JAR, thrown);
        }
    }

    @Override
    public void dropSQLJJar(Session session, TableName jarName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_SQLJ_JAR);
            delegate.dropSQLJJar(session, jarName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_SQLJ_JAR, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_SQLJ_JAR, thrown);
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
    public void dropGroup(Session session, TableName groupName) {
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
    public TypesRegistry getTypesRegistry() {
        return delegate.getTypesRegistry();
    }

    @Override
    public TypesTranslator getTypesTranslator() {
        return delegate.getTypesTranslator();
    }

    @Override
    public StorageFormatRegistry getStorageFormatRegistry() {
        return delegate.getStorageFormatRegistry();
    }

    @Override
    public AISCloner getAISCloner() {
        return delegate.getAISCloner();
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
    public RowDef getRowDef(Session session, int tableId) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_ROWDEF);
            return delegate.getRowDef(session, tableId);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_ROWDEF, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_ROWDEF, thrown);
        }
    }

   @Override
    public int getGenerationAsInt(Session session) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_SCHEMA_ID);
            return delegate.getGenerationAsInt(session);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_SCHEMA_ID, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_SCHEMA_ID, thrown);
        }
    }

    @Override
    public long getGeneration(Session session) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_SCHEMA_TIMESTAMP);
            return delegate.getGeneration(session);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_SCHEMA_TIMESTAMP, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.GET_SCHEMA_TIMESTAMP, thrown);
        }
    }

    @Override
    public long getOldestActiveGeneration() {
        Session session = sessionService.createSession();
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_OLDEST_ACTIVE_GENERATION);
            return delegate.getOldestActiveGeneration();
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_OLDEST_ACTIVE_GENERATION, t);
            throw throwAlways(t);
        } finally {
            try {
                hook.hookFunctionFinally(session, DXLFunction.GET_OLDEST_ACTIVE_GENERATION, thrown);
            } finally {
                session.close();
            }
        }
    }

    @Override
    public Set<Long> getActiveGenerations() {
        Session session = sessionService.createSession();
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.GET_ACTIVE_GENERATIONS);
            return delegate.getActiveGenerations();
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.GET_ACTIVE_GENERATIONS, t);
            throw throwAlways(t);
        } finally {
            try {
                hook.hookFunctionFinally(session, DXLFunction.GET_ACTIVE_GENERATIONS, thrown);
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
    public void dropGroupIndexes(Session session, TableName groupName, Collection<String> indexesToDrop) {
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

    @Override
    public void createSequence(Session session, Sequence sequence) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CREATE_SEQUENCE);
            delegate.createSequence(session, sequence);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.CREATE_SEQUENCE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CREATE_SEQUENCE, thrown);
        }
        
    }

    @Override
    public void dropSequence(Session session, TableName sequenceName) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.DROP_SEQUENCE);
            delegate.dropSequence(session, sequenceName);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.DROP_SEQUENCE, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.DROP_SEQUENCE, thrown);
        }
    }

    /** Test only, not hooked. */
    @Override
    public void setOnlineDDLMonitor(OnlineDDLMonitor onlineDDLMonitor) {
        delegate.setOnlineDDLMonitor(onlineDDLMonitor);
    }
}
