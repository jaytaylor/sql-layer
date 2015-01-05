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

package com.foundationdb.qp.loadableplan.std;

import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.std.DumpGroupLoadablePlan;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.sql.TestBase;
import com.foundationdb.sql.embedded.EmbeddedJDBCITBase;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import java.sql.Statement;
import java.io.File;
import java.util.Collection;
import java.util.List;

@RunWith(NamedParameterizedRunner.class)
public class DumpGroupLoadablePlanIT extends EmbeddedJDBCITBase
{
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + DumpGroupLoadablePlan.class.getPackage().getName().replace('.', '/'));
    public static final File SCHEMA_FILE = new File(RESOURCE_DIR, "schema.ddl");
    public static final String GROUP_NAME = "customers";

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        pb.add("single", new File(RESOURCE_DIR, GROUP_NAME + ".sql"), GROUP_NAME, false, -1);
        pb.add("single/commit", new File(RESOURCE_DIR, GROUP_NAME + ".sql"),GROUP_NAME, false, 1);
        pb.add("multiple", new File(RESOURCE_DIR, GROUP_NAME + "-m.sql"),GROUP_NAME, true, -1);
        pb.add("multiple/commit", new File(RESOURCE_DIR, GROUP_NAME + "-m.sql"),GROUP_NAME, true, 1);
        pb.add("values", new File(RESOURCE_DIR, "values.sql"), "values", true, 1);
        pb.add("guid", new File(RESOURCE_DIR, "guid.sql"), "guid_table", true, 1);
        pb.add("strings", new File(RESOURCE_DIR, "strings.sql"), "strings", true, 1);
        return pb.asList();
    }

    private File file;
    private boolean multiple;
    private int commitFreq;
    private String groupName;

    public DumpGroupLoadablePlanIT(File file, String groupName, boolean multiple, int commitFreq) {
        this.file = file;
        this.multiple = multiple;
        this.commitFreq = commitFreq;
        this.groupName = groupName;
    }

    @Before
    public void loadDatabase() throws Exception {
        loadSchemaFile(SCHEMA_NAME, SCHEMA_FILE);
    }

    @Test
    public void testDump() throws Exception {
        String expectedSQL = runSQL();
        try(CloseableTransaction txn = txnService().beginCloseableTransaction(session())) {
            runPlan(expectedSQL);
            txn.commit();
        }
    }

    private String runSQL() throws Exception {
        // Run the INSERTs via SQL.
        String sql = TestBase.fileContents(file);

        Statement stmt = getConnection().createStatement();
        for (String sqls : sql.split("\\;\\s*")) {
            stmt.execute(sqls);
        }
        stmt.close();
        return sql;
    }

    private void runPlan(String expectedSQL) throws Exception {
        DumpGroupLoadablePlan loadablePlan = new DumpGroupLoadablePlan();
        DirectObjectPlan plan = loadablePlan.plan();

        StoreAdapter adapter = newStoreAdapter();
        QueryContext queryContext = new SimpleQueryContext(adapter) {
                @Override
                public String getCurrentSchema() {
                    return SCHEMA_NAME;
                }
            };
        QueryBindings queryBindings = queryContext.createBindings();
        queryBindings.setValue(0, new Value(MString.varcharFor(SCHEMA_NAME), SCHEMA_NAME));
        queryBindings.setValue(1, new Value(MString.varcharFor(groupName), groupName));
        if (multiple)
            queryBindings.setValue(2, new Value(MNumeric.INT.instance(false), 10));
        if (commitFreq > 0)
            queryBindings.setValue(3, new Value(MNumeric.INT.instance(false), commitFreq));

        DirectObjectCursor cursor = plan.cursor(queryContext, queryBindings);
        
        StringBuilder actual = new StringBuilder();

        cursor.open();
        while(true) {
            List<?> columns = cursor.next();
            if (columns == null) {
                break;
            }
            else if (!columns.isEmpty()) {
                assertTrue(columns.size() == 1);
                if (actual.length() > 0)
                    actual.append("\n");
                actual.append(columns.get(0));
            }
        }
        cursor.close();

        assertEquals(expectedSQL, actual.toString());
    }

}
