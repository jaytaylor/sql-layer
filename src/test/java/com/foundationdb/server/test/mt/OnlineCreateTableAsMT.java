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

package com.foundationdb.server.test.mt;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.API.SortOption;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.User;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.sql.optimizer.rule.cost.TestCostEstimator;
import com.foundationdb.sql.server.*;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

/** Interleaved DML during an online create index for a single table. */
public class OnlineCreateTableAsMT extends OnlineMTBase {
    private static final String SCHEMA = "test";
    private static final String FROM_TABLE = "ft";
    private static final String TO_TABLE = "tt";
    private static final String INDEX_NAME = "x";
    private static final String CREATE_QUERY = " CREATE TABLE " + TO_TABLE + " AS SELECT * FROM " + FROM_TABLE + " WITH DATA ";
    private int tID;
    private int ntID;

    TableRowType tableRowType;
    List<Row> groupRows;
    List<Row> otherGroupRows;
    List<String> columnNames;
    List<DataTypeDescriptor> descriptors;
    TestSession server;



    @Before
    public void createAndLoad() {
        tID = createTable(SCHEMA, FROM_TABLE, "id INT NOT NULL PRIMARY KEY, x INT");

        tableRowType = SchemaCache.globalSchema(ais()).tableRowType(tID);

        writeRows(createNewRow(tID, 1, 10),
                createNewRow(tID, 2, 20),
                createNewRow(tID, 3, 30),
                createNewRow(tID, 4, 40));

        groupRows = runPlanTxn(groupScanCreator(tID));//runs given plan and returns output row
        columnNames = Arrays.asList("id", "x");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        descriptors = Arrays.asList(d,d);
        server = new TestSession();



    }
    @Override
    protected String getDDL() {
        return CREATE_QUERY;
    }

    @Override
    protected String getDDLSchema() {
        return SCHEMA;
    }

    @Override
    protected List<Row> getGroupExpected() {
        return groupRows;
    }

    @Override
    protected List<Row> getOtherExpected() {
        // Generate what should be in the table index from the group rows
        return otherGroupRows;
    }
    @Override
    protected OperatorCreator getGroupCreator() {
        return groupScanCreator(tID);
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        ntID = ais().getTable(SCHEMA, TO_TABLE).getTableId();


        return groupScanCreator(ntID);
    }

    @Override
    protected void postCheckAIS(AkibanInformationSchema ais) {
        Table newTable = ais().getTable(tID);
        assertNotNull("new table", newTable);
    }
    //
    // I/U/D pre-to-post METADATA
    //

    @Test
    public void insertPreToPostMetadata() {
        Row newRow = testRow(tableRowType, 5, 50);

        dmlPreToPostMetadata(insertCreator(tID, newRow),getGroupExpected(), true, descriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void updatePreToPostMetadata() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);

        dmlPreToPostMetadata(updateCreator(tID, oldRow, newRow), getGroupExpected(), true, descriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void deletePreToPostMetadata() {
        Row oldRow = groupRows.get(0);

        dmlPreToPostMetadata(deleteCreator(tID, oldRow), getGroupExpected(), true, descriptors, columnNames, server, CREATE_QUERY, true);
    }

    //
    // I/U/D post METADATA to pre FINAL
    //

    @Test
    public void insertPostMetaToPreFinal() {
        Row newRow = testRow(tableRowType, 5, 50);
        otherGroupRows = combine(groupRows, newRow);
        dmlPostMetaToPreFinal(insertCreator(tID, newRow), combine(groupRows, newRow), true, true, descriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Ignore
    public void updatePostMetaToPreFinal() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        otherGroupRows = replace(groupRows, 1, newRow);
        dmlPostMetaToPreFinal(updateCreator(tID, oldRow, newRow), replace(groupRows, 1, newRow), true, true, descriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Ignore
    public void deletePostMetaToPreFinal() {
        Row oldRow = groupRows.get(0);
        otherGroupRows = groupRows.subList(1, groupRows.size());
        dmlPostMetaToPreFinal(deleteCreator(tID, oldRow), groupRows.subList(1, groupRows.size()), true, true, descriptors, columnNames, server, CREATE_QUERY, true);
    }

    //
    // I/U/D pre-to-post FINAL
    //


    @Test
    public void insertPreToPostFinal() {
        Row newRow = testRow(tableRowType, 5, 50);
        dmlPreToPostFinal(insertCreator(tID, newRow),getGroupExpected(), true, descriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void updatePreToPostFinal() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        dmlPreToPostFinal(updateCreator(tID, oldRow, newRow), getGroupExpected(), true, descriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void deletePreToPostFinal() {
        Row oldRow = groupRows.get(0);
        dmlPreToPostFinal(deleteCreator(tID, oldRow), getGroupExpected(), true, descriptors, columnNames, server, CREATE_QUERY, true);
    }

    protected List<String> warnings = null;

    protected List<String> getWarnings() {
        return warnings;
    }

    protected class TestOperatorCompiler extends ServerOperatorCompiler {
        public TestOperatorCompiler(TestSession session) {
            initServer(session, store());
            initDone();
        }
    }
    public class TestSession extends ServerSessionBase {
        public TestSession() {
            super(new ServerServiceRequirements(serviceManager().getLayerInfo(),
                    dxl(),
                    serviceManager().getMonitorService(),
                    serviceManager().getSessionService(),
                    store(),
                    configService(),
                    serviceManager().getServiceByClass(IndexStatisticsService.class),
                    serviceManager().getServiceByClass(TypesRegistryService.class),
                    routineLoader(),
                    txnService(),
                    new DummySecurityService(),
                    new TestCostEstimator.TestCostModelFactory(),
                    serviceManager().getServiceByClass(MetricsService.class),
                    serviceManager()));
            session = session();
            ais = ais();
            defaultSchemaName = SCHEMA;
            properties = new Properties();
            properties.put("database", defaultSchemaName);
            properties.put(CONFIG_PARSER_FEATURES, configService().getProperty("fdbsql.postgres." + CONFIG_PARSER_FEATURES));
            initParser();
            TestOperatorCompiler compiler = new TestOperatorCompiler(this);
            initAdapters(compiler);
        }

        @Override
        protected void sessionChanged() {
        }

        public void setSession(Session session){
            this.session = session;
        }

        @Override
        public void notifyClient(QueryContext.NotificationLevel level, ErrorCode errorCode, String message) {
            if (warnings == null)
                warnings = new ArrayList<>();
            warnings.add(message);
        }

    }protected static class DummySecurityService implements SecurityService {
        @Override
        public User authenticate(Session session, String name, String password) {
            return null;
        }

        @Override
        public User authenticate(Session session, String name, String password, byte[] salt) {
            return null;
        }

        @Override
        public boolean isAccessible(Session session, String schema) {
            return true;
        }

        @Override
        public boolean isAccessible(javax.servlet.http.HttpServletRequest request, String schema) {
            return true;
        }

        @Override
        public boolean hasRestrictedAccess(Session session) {
            return true;
        }

        @Override
        public void addRole(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRole(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public User getUser(String name) {
            return null;
        }

        @Override
        public User addUser(String name, String password, java.util.Collection<String> roles) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteUser(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void changeUserPassword(String name, String password) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearAll(Session session) {
        }
    }

}