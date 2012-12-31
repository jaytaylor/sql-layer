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

package com.akiban.server.service.externaldata;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.util.Strings;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

public final class MysqlDumpRowReaderTest {
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + MysqlDumpRowReaderTest.class.getPackage().getName().replace('.', '/'));
    public static final File DUMP_FILE = new File(RESOURCE_DIR, "t1.sql");
    
    public static final Object[][] ROWS = {
        { 2, null },
        { 3, "a 'xyz' b" },
        { 5, "abc\nxyz" },
        { 1, "foo" },
        { 6, "\u2603" }
    };

    public static final String DDL =
        "CREATE TABLE t1(id INT NOT NULL PRIMARY KEY, s VARCHAR(32))";

    @Test
    public void reader() throws Exception {
        SchemaFactory schemaFactory = new SchemaFactory("test");
        AkibanInformationSchema ais = schemaFactory.aisWithRowDefs(DDL);
        UserTable t1 = ais.getUserTable("test", "t1");
        InputStream istr = new FileInputStream(DUMP_FILE);
        MysqlDumpRowReader reader = new MysqlDumpRowReader(t1, t1.getColumns(), 
                                                           istr, "UTF-8",
                                                           null);
        List<NewRow> rows = new ArrayList<NewRow>();
        NewRow row;
        while ((row = reader.nextRow()) != null)
            rows.add(row);
        istr.close();
        assertEquals("number of rows", ROWS.length, rows.size());
        for (int i = 0; i < ROWS.length; i++) {
            Object[] orow = ROWS[i];
            row = rows.get(i);
            assertEquals("row " + i + " size", orow.length, row.getRowDef().getFieldCount());
            for (int j = 0; j < orow.length; j++) {
                assertEquals("row " + i + " col " + j, orow[j], row.get(j));
            }
        }
    }

}
