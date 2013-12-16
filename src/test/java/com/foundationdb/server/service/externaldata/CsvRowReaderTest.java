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
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.util.Strings;

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
        { 1, 20100101000000L, 100, "abc" },
        { 2, null, 100, "xyz" },
        { 666, 20130101130203L, 100, "quoted \"string\" here"}
    };

    public static final String DDL =
        "CREATE TABLE t1(id INT NOT NULL PRIMARY KEY, t TIMESTAMP, n INT DEFAULT 100, s VARCHAR(128))";

    @Test
    public void reader() throws Exception {
        SchemaFactory schemaFactory = new SchemaFactory("test");
        AkibanInformationSchema ais = schemaFactory.aisWithRowDefs(DDL);
        Table t1 = ais.getTable("test", "t1");
        InputStream istr = new ByteArrayInputStream(Strings.join(CSV).getBytes("UTF-8"));
        List<Column> columns = new ArrayList<>(3);
        for (String cname : CSV[0].split(","))
            columns.add(t1.getColumn(cname));
        CsvRowReader reader = new CsvRowReader(t1, columns,
                                               istr, new CsvFormat("UTF-8"), 
                                               null, MTypesTranslator.INSTANCE);
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
