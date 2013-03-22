
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
        List<NewRow> rows = new ArrayList<>();
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
