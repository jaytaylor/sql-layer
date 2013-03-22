/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.pg;

import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.loadableplan.std.DumpGroupLoadablePlan;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.sql.TestBase;
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
                 + DumpGroupLoadablePlan.class.getPackage().getName().replace('.', '/'));
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
        loadSchemaFile(SCHEMA_NAME, SCHEMA_FILE);
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

        Schema schema = new Schema(ais());
        PersistitAdapter adapter = persistitAdapter(schema);
        QueryContext queryContext = new SimpleQueryContext(adapter) {
                @Override
                public String getCurrentSchema() {
                    return SCHEMA_NAME;
                }
            };
        if (Types3Switch.ON) {
            queryContext.setPValue(0, new PValue(MString.varcharFor(SCHEMA_NAME), SCHEMA_NAME));
            queryContext.setPValue(1, new PValue(MString.varcharFor(GROUP_NAME), GROUP_NAME));
            if (multiple)
                queryContext.setPValue(2, new PValue(MNumeric.INT.instance(false), 10));
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

        assertEquals(sql, actual.toString());
    }

}
