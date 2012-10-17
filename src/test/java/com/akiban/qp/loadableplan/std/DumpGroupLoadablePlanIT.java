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

package com.akiban.qp.loadableplan.std;

import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.Schema;
import com.akiban.sql.TestBase;
import com.akiban.sql.pg.PostgresServerFilesITBase;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValue;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import java.sql.Statement;
import java.io.File;
import java.util.Collection;
import java.util.List;

@RunWith(NamedParameterizedRunner.class)
public class DumpGroupLoadablePlanIT extends PostgresServerFilesITBase
{
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + DumpGroupLoadablePlanIT.class.getPackage().getName().replace('.', '/'));
    public static final File SCHEMA_FILE = new File(RESOURCE_DIR, "schema.ddl");
    public static final String GROUP_NAME = "customers";

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        pb.add("single", new File(RESOURCE_DIR, GROUP_NAME + ".sql"), false);
        pb.add("multiple", new File(RESOURCE_DIR, GROUP_NAME + "-m.sql"), true);
        return pb.asList();
    }

    private File file;
    private boolean multiple;

    public DumpGroupLoadablePlanIT(File file, boolean multiple) {
        this.file = file;
        this.multiple = multiple;
    }

    @Before
    public void loadDatabase() throws Exception {
        loadSchemaFile(SCHEMA_FILE);
    }

    @Test
    public void testDump() throws Exception {
        // Run the INSERTs via SQL.
        String sql = TestBase.fileContents(file);

        Statement stmt = getConnection().createStatement();
        for (String sqls : sql.split("\\;\\s*")) {
            stmt.execute(sqls);
        }
        stmt.close();

        DumpGroupLoadablePlan loadablePlan = new DumpGroupLoadablePlan();
        DirectObjectPlan plan = loadablePlan.plan();

        Schema schema = new Schema(rowDefCache().ais());
        PersistitAdapter adapter = persistitAdapter(schema);
        QueryContext queryContext = new SimpleQueryContext(adapter) {
                @Override
                public String getCurrentUser() {
                    return SCHEMA_NAME;
                }
            };
        if (Types3Switch.ON) {
            queryContext.setPValue(0, new PValue(SCHEMA_NAME));
            queryContext.setPValue(1, new PValue(GROUP_NAME));
            if (multiple)
                queryContext.setPValue(2, new PValue(10));
        }
        else {
            queryContext.setValue(0, new FromObjectValueSource().setReflectively(SCHEMA_NAME));
            queryContext.setValue(1, new FromObjectValueSource().setReflectively(GROUP_NAME));
            if (multiple)
                queryContext.setValue(2, new FromObjectValueSource().setReflectively(10L));
        }

        DirectObjectCursor cursor = plan.cursor(queryContext);
        
        StringBuilder actual = new StringBuilder();

        cursor.open();
        while(true) {
            List<? extends Object> columns = cursor.next();
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

        assertEquals(sql, actual.toString());
    }

}
