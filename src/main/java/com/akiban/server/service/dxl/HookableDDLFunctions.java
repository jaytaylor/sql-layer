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

package com.akiban.server.service.dxl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.util.TableChange;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;

import java.util.Collection;
import java.util.List;

import static com.akiban.ais.util.TableChangeValidator.ChangeLevel;
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
    public ChangeLevel alterTable(Session session, TableName tableName, UserTable newDefinition,
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
    public void createRoutine(Session session, Routine routine) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CREATE_ROUTINE);
            delegate.createRoutine(session, routine);
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
            hook.hookFunctionIn(session, DXLFunction.GET_SCHEMA_TIMESTAMP);
            return delegate.getGeneration(session);
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
    public IndexCheckSummary checkAndFixIndexes(Session session, String schemaRegex, String tableRegex) {
        Throwable thrown = null;
        try {
            hook.hookFunctionIn(session, DXLFunction.CHECK_AND_FIX_INDEXES);
            return delegate.checkAndFixIndexes(session, schemaRegex, tableRegex);
        } catch (Throwable t) {
            thrown = t;
            hook.hookFunctionCatch(session, DXLFunction.CHECK_AND_FIX_INDEXES, t);
            throw throwAlways(t);
        } finally {
            hook.hookFunctionFinally(session, DXLFunction.CHECK_AND_FIX_INDEXES, thrown);
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
}
