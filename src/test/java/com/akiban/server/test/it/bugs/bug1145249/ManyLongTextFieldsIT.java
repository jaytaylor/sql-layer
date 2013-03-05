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

package com.akiban.server.test.it.bugs.bug1145249;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

public class ManyLongTextFieldsIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";

    private void runTest(int fieldCount) {
        AISBuilder builder = new AISBuilder();
        builder.userTable(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "id", 0, "int", null, null, false, false, null, null);
        builder.index(SCHEMA, TABLE, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, TABLE, Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);
        for(int i = 1; i <= fieldCount; ++i) {
            String colName = "lt_" + i;
            builder.column(SCHEMA, TABLE, colName, i, "LONGTEXT", null, null, true, false, null, null);
        }
        UserTable table = builder.akibanInformationSchema().getUserTable(SCHEMA, TABLE);
        ddl().createTable(session(), table);
        updateAISGeneration();

        Object[] colValues = new Object[fieldCount + 1];
        colValues[0] = 1L;
        for(int i = 1; i <= fieldCount; ++i) {
            colValues[i] = "longtext_value";
        }

        int tid = tableId(SCHEMA, TABLE);
        NewRow row = createNewRow(tid, colValues);
        writeRows(row);
        expectFullRows(tid, row);
    }

    @Test
    public void oneField() {
        runTest(1);
    }

    @Test
    public void fiveFields() {
        runTest(5);
    }

    @Test
    public void tenFields() {
        runTest(10);
    }

    @Test
    public void twentyFields() {
        runTest(20);
    }

    @Test
    public void fiftyFields() {
        runTest(50);
    }
}
