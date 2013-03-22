
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

public final class CsvRowReaderTest {
    public static final String[] CSV = {
        "id,s,t",
        "1,abc,2010-01-01",
        "2,xyz,",
        "666,\"quoted \"\"string\"\" here\",2013-01-01 13:02:03"
    };

    public static final Object[][] ROWS = {
        { 1, "abc", 20100101000000L },
        { 2, "xyz", null },
        { 666, "quoted \"string\" here", 20130101130203L}
    };

    public static final String DDL =
        "CREATE TABLE t1(id INT NOT NULL PRIMARY KEY, s VARCHAR(128), t TIMESTAMP)";

    @Test
    public void reader() throws Exception {
        SchemaFactory schemaFactory = new SchemaFactory("test");
        AkibanInformationSchema ais = schemaFactory.aisWithRowDefs(DDL);
        UserTable t1 = ais.getUserTable("test", "t1");
        InputStream istr = new ByteArrayInputStream(Strings.join(CSV).getBytes("UTF-8"));
        CsvRowReader reader = new CsvRowReader(t1, t1.getColumns(),
                                               istr, new CsvFormat("UTF-8"), 
                                               null);
        reader.skipRows(1); // Header
        List<NewRow> rows = new ArrayList<>();
        NewRow row;
        while ((row = reader.nextRow()) != null)
            rows.add(row);
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
