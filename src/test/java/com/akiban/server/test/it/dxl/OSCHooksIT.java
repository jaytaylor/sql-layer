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

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.pt.OSCAlterTableHook;
import com.akiban.ais.pt.OSCRenameTableHook;
import com.akiban.ais.util.TableChange;
import com.akiban.message.MessageRequiredServices;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.config.Property;
import com.akiban.server.service.dxl.IndexCheckSummary;
import com.akiban.server.service.session.Session;

import static com.akiban.ais.util.TableChangeValidator.ChangeLevel;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.*;

/**
 * Online schema change hooks
 */
public class OSCHooksIT extends AlterTableITBase {
    private static final String _O_NEW_TABLE = "_o_new";
    private static final TableName _O_NEW_NAME = new TableName(SCHEMA, _O_NEW_TABLE);
    private static final String _O_OLD_TABLE = "_o_old";
    private static final TableName _O_OLD_NAME = new TableName(SCHEMA, _O_OLD_TABLE);
    
    @Before
    public void createCOI() {
        String o_cols = "oid int not null primary key, cid int, order_date date, extra smallint";
        createTable(C_NAME, "cid int not null primary key, name varchar(128)");
        createTable(O_NAME, o_cols + "," + akibanFK("cid", "c", "cid"));
        createTable(I_NAME, "iid int not null primary key, oid int, sku varchar(10)," + 
                    akibanFK("oid", "o", "oid"));
        
        createTable(_O_NEW_NAME, o_cols);
    }

    @Test
    public void testAddCol() {
        runAlter("ALTER TABLE \"_o_new\" ADD COLUMN s2 VARCHAR(16)");
        ddlForAlter().renameTable(session(), O_NAME, _O_OLD_NAME);
        ddlForAlter().renameTable(session(), _O_NEW_NAME, O_NAME);
        assertEquals("same columns", tableColumns(_O_OLD_NAME), tableColumns(O_NAME));
        assertEquals("C-O same group", tableGroup(C_NAME), tableGroup(O_NAME));
        assertEquals("O-I same group", tableGroup(O_NAME), tableGroup(I_NAME));
    }

    @Test
    public void testModifyCol() {
        runAlter("ALTER TABLE \"_o_new\" ALTER COLUMN order_date SET DATA TYPE timestamp");
        ddlForAlter().renameTable(session(), O_NAME, _O_OLD_NAME);
        ddlForAlter().renameTable(session(), _O_NEW_NAME, O_NAME);
        assertEquals("same columns", tableColumns(_O_OLD_NAME), tableColumns(O_NAME));
        assertEquals("C-O same group", tableGroup(C_NAME), tableGroup(O_NAME));
        assertEquals("O-I same group", tableGroup(O_NAME), tableGroup(I_NAME));
    }

    @Test
    public void testDropCol() {
        runAlter("ALTER TABLE \"_o_new\" DROP COLUMN extra");
        ddlForAlter().renameTable(session(), O_NAME, _O_OLD_NAME);
        ddlForAlter().renameTable(session(), _O_NEW_NAME, O_NAME);
        assertEquals("same columns", tableColumns(_O_OLD_NAME), tableColumns(O_NAME));
        assertEquals("C-O same group", tableGroup(C_NAME), tableGroup(O_NAME));
        assertEquals("O-I same group", tableGroup(O_NAME), tableGroup(I_NAME));
    }

    @Test
    public void testDropFK() {
        runAlter("ALTER TABLE \"_o_new\" DROP COLUMN cid");
        ddlForAlter().renameTable(session(), O_NAME, _O_OLD_NAME);
        ddlForAlter().renameTable(session(), _O_NEW_NAME, O_NAME);
        assertEquals("same columns", tableColumns(_O_OLD_NAME), tableColumns(O_NAME));
        assertFalse("C-O split group", tableGroup(C_NAME).equals(tableGroup(O_NAME)));
        assertEquals("O-I same group", tableGroup(O_NAME), tableGroup(I_NAME));
    }

    @Test
    public void testRenameFK() {
        runRenameColumn(_O_NEW_NAME, "cid", "pid");
        ddlForAlter().renameTable(session(), O_NAME, _O_OLD_NAME);
        ddlForAlter().renameTable(session(), _O_NEW_NAME, O_NAME);
        assertEquals("same columns", tableColumns(_O_OLD_NAME), tableColumns(O_NAME));
        assertEquals("C-O same group", tableGroup(C_NAME), tableGroup(O_NAME));
        assertEquals("O-I same group", tableGroup(O_NAME), tableGroup(I_NAME));
    }

    private Group tableGroup(TableName name) {
        return ais().getUserTable(name).getGroup();
    }

    private List<String> tableColumns(TableName name) {
        List<String> result = new ArrayList<String>();
        for (Column column : ais().getUserTable(name).getColumns()) {
            result.add(column.getName());
        }
        return result;
    }

    /* Apply hooks in test DDL flow so that we can intercept without the adapter. */

    @Override
    protected Collection<Property> startupConfigProperties() {
        return Collections.singletonList(new Property("akserver.pt.osc.hook", "enabled"));
    }

    private DDLFunctions wrappedDDL = null;

    private class WrappedDDLFunctions implements DDLFunctions {
        private final DDLFunctions delegate;
        private final OSCAlterTableHook alterHook;
        private final OSCRenameTableHook renameHook;

        public WrappedDDLFunctions(DDLFunctions delegate) {
            this.delegate = delegate;
            MessageRequiredServices reqs =
                new MessageRequiredServices(store(),
                                            serviceManager().getSchemaManager(),
                                            dxl(),
                                            configService(),
                                            serviceManager().getStatisticsService(),
                                            serviceManager().getSessionService());
            alterHook = new OSCAlterTableHook(reqs);
            renameHook = new OSCRenameTableHook(reqs);
        }

        @Override
        public void createTable(Session session, UserTable table) {
            delegate.createTable(session, table);
        }

        @Override
        public void renameTable(Session session, TableName currentName, TableName newName) {
            if (renameHook.before(session, currentName, newName))
                delegate.renameTable(session, currentName, newName);
        }

        @Override
        public void dropTable(Session session, TableName tableName) {
            delegate.dropTable(session, tableName);
        }

        @Override
        public ChangeLevel alterTable(Session session, TableName tableName, UserTable newDefinition,
                                      List<TableChange> columnChanges, List<TableChange> indexChanges,
                                      QueryContext context) {
            alterHook.before(session, tableName, newDefinition, columnChanges, indexChanges);
            return delegate.alterTable(session, tableName, newDefinition, columnChanges, indexChanges, context);
        }

        @Override
        public void createView(Session session, View view) {
            delegate.createView(session, view);
        }

        @Override
        public void dropView(Session session, TableName viewName) {
            delegate.dropView(session, viewName);
        }

        @Override
        public void dropSchema(Session session, String schemaName) {
            delegate.dropSchema(session, schemaName);
        }

        @Override
        public void dropGroup(Session session, String groupName) {
            delegate.dropGroup(session, groupName);
        }

        @Override
        public AkibanInformationSchema getAIS(final Session session) {
            return delegate.getAIS(session);
        }

        @Override
        public int getTableId(Session session, TableName tableName) {
            return delegate.getTableId(session, tableName);
        }

        @Override
        public Table getTable(Session session, int tableId) {
            return delegate.getTable(session, tableId);
        }

        @Override
        public Table getTable(Session session, TableName tableName) {
            return delegate.getTable(session, tableName);
        }

        @Override
        public UserTable getUserTable(Session session, TableName tableName) {
            return delegate.getUserTable(session, tableName);
        }

        @Override
        public TableName getTableName(Session session, int tableId) {
            return delegate.getTableName(session, tableId);
        }

        @Override
        public RowDef getRowDef(int tableId) {
            return delegate.getRowDef(tableId);
        }

        @Override
        public List<String> getDDLs(final Session session) {
            return delegate.getDDLs(session);
        }

        @Override
        public int getGeneration() {
            return delegate.getGeneration();
        }

        @Override
        public long getTimestamp() {
            return delegate.getTimestamp();
        }

        @Override
        public void createIndexes(final Session session, Collection<? extends Index> indexesToAdd) {
            delegate.createIndexes(session, indexesToAdd);
        }

        @Override
        public void dropTableIndexes(final Session session, TableName tableName, Collection<String> indexNamesToDrop) {
            delegate.dropTableIndexes(session, tableName, indexNamesToDrop);
        }

        @Override
        public void dropGroupIndexes(Session session, String groupName, Collection<String> indexesToDrop) {
            delegate.dropGroupIndexes(session, groupName, indexesToDrop);
        }

        @Override
        public void updateTableStatistics(Session session, TableName tableName, Collection<String> indexesToUpdate) {
            delegate.updateTableStatistics(session, tableName, indexesToUpdate);
        }

        @Override
        public IndexCheckSummary checkAndFixIndexes(Session session, String schemaRegex, String tableRegex) {
            return delegate.checkAndFixIndexes(session, schemaRegex, tableRegex);
        }

        @Override
        public void createSequence(Session session, Sequence sequence) {
            delegate.createSequence(session, sequence);
        
        }

        @Override
        public void dropSequence(Session session, TableName sequenceName) {
            delegate.dropSequence(session, sequenceName);
        }

        @Override
        public void createRoutine(Session session, Routine routine) {
            delegate.createRoutine(session, routine);
        
        }

        @Override
        public void dropRoutine(Session session, TableName routineName) {
            delegate.dropRoutine(session, routineName);
        }
    }

    @Override
    protected DDLFunctions ddlForAlter() {
        if (wrappedDDL == null)
            wrappedDDL = new WrappedDDLFunctions(super.ddlForAlter());
        return wrappedDDL;
    }
    
    private AkibanInformationSchema ais() {
        return ddl().getAIS(session());
    }

}
