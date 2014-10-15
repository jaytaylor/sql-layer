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

package com.foundationdb.server.service.externaldata;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.util.Strings;

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
        Table t1 = ais.getTable("test", "t1");
        InputStream istr = new FileInputStream(DUMP_FILE);
        MysqlDumpRowReader reader = new MysqlDumpRowReader(t1, t1.getColumns(), 
                                                           istr, "UTF-8",
                                                           null, MTypesTranslator.INSTANCE);
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
