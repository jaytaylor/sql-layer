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

package com.akiban.sql.pg;

import com.akiban.sql.TestBase;

import com.akiban.server.rowdata.RowDef;

import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.sql.RegexFilenameFilter;

import org.junit.Ignore;

import java.io.File;
import java.util.EnumSet;

/**
 * A base class for integration tests that use data from files to specify the
 * input and output expected from calls to the Postgres server.
 */
@Ignore
public class PostgresServerFilesITBase extends PostgresServerITBase
{
    public void loadDatabase(File dir) throws Exception {
        rootTableId = loadSchemaFile(SCHEMA_NAME, new File(dir, "schema.ddl"));
        for (File data : dir.listFiles(new RegexFilenameFilter(".*\\.dat"))) {
            loadDataFile(SCHEMA_NAME, data);
        }
    }

    protected int rootTableId;

    protected String dumpData() throws Exception {
        final StringBuilder str = new StringBuilder();
        CursorId cursorId = dml()
            .openCursor(session(), aisGeneration(), 
                        new ScanAllRequest(rootTableId, null, 0,
                                           EnumSet.of(ScanFlag.DEEP)));
        dml().scanSome(session(), cursorId,
                       new RowOutput() {
                           public void output(NewRow row) {
                               RowDef rowDef = row.getRowDef();
                               str.append(rowDef.table().getName().getTableName());
                               for (int i = 0; i < rowDef.getFieldCount(); i++) {
                                   str.append(",");
                                   str.append(row.get(i));
                               }
                               str.append("\n");
                           }

                           public void mark() {
                           }
                           public void rewind() {
                           }
                       });
        dml().closeCursor(session(), cursorId);
        return str.toString();
    }

    protected String caseName, sql, expected, error;
    protected String[] params;

    /** Parameterized version. */
    protected PostgresServerFilesITBase(String caseName, String sql, 
					String expected, String error,
					String[] params) {
        this.caseName = caseName;
        this.sql = sql.trim();
        this.expected = expected;
        this.error = error;
        this.params = params;
    }

    protected PostgresServerFilesITBase() {
    }

    protected void generateAndCheckResult() throws Exception {
        TestBase.generateAndCheckResult((TestBase.GenerateAndCheckResult)this, 
                                        caseName, expected, error);
    }

}
