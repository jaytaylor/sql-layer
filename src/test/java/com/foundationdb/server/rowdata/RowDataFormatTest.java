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

package com.foundationdb.server.rowdata;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.ais.model.aisb2.NewTableBuilder;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.util.Strings;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class RowDataFormatTest {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();

        // fixed length
        builder.add(
                "id int => 1",
                new TableMaker() { public void make(NewTableBuilder b) { b.colInt("id"); } },
                fields(1L),
                "0x1B00 0000 4142 0200 0100 0000 0001 0000 0001 0000 0042 411B 0000 00"
        );
        builder.add(
                "id int => null",
                new TableMaker() { public void make(NewTableBuilder b) { b.colInt("id"); } },
                fields(NULL),
                "0x1700 0000 4142 0200 0100 0000 0201 0000 0042 4117 0000 00"
        );
        // var length
        builder.add(
                "name varchar(32) => str(1)",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32); } },
                fields(str(1)),
                "0x1A00 0000 4142 0200 0100 0000 0001 0000 0002 0130 4241 1A00 0000"
        );
        builder.add(
                "name varchar(32) => null",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32); } },
                fields(NULL),
                "0x1700 0000 4142 0200 0100 0000 0201 0000 0042 4117 0000 00"
        );
        builder.add(
                "name varchar(32) => str(32)",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32); } },
                fields(str(32)),
                "0x3900 0000 4142 0200 0100 0000 0001 0000 0021 2030 3132 3334 3536 3738 3930 3132 3334 3536 3738 "
                        +"3930 3132 3334 3536 3738 3930 3142 4139 0000 00"
        );
        builder.add(
                "name varchar(1024) => str(1)",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 1024); } },
                fields(str(1)),
                "0x1C00 0000 4142 0200 0100 0000 0001 0000 0003 0001 0030 4241 1C00 0000"
        );
        // mixed
        builder.add(
                "name varchar(32), id int => str(1), 1",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32).colInt("id"); } },
                fields(str(1), 1L),
                "0x1E00 0000 4142 0300 0100 0000 0001 0000 0002 0100 0000 0130 4241 1E00 0000"
        );
        builder.add(
                "id int, name varchar(32) => 1, str(1)",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32).colInt("id"); } },
                fields(str(1), 1L),
                "0x1E00 0000 4142 0300 0100 0000 0001 0000 0002 0100 0000 0130 4241 1E00 0000"
        );
        builder.add(
                "name varchar(32), id int => null, 1",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32).colInt("id"); } },
                fields(NULL, 1L),
                "0x1B00 0000 4142 0300 0100 0000 0201 0000 0001 0000 0042 411B 0000 00"
        );
        builder.add(
                "id int, name varchar(32) => 1, null",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32).colInt("id"); } },
                fields(NULL, 1L),
                "0x1B00 0000 4142 0300 0100 0000 0201 0000 0001 0000 0042 411B 0000 00"
        );
        builder.add(
                "id int, name varchar(32) => null, null",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32).colInt("id"); } },
                fields(NULL, NULL),
                "0x1700 0000 4142 0300 0100 0000 0601 0000 0042 4117 0000 00"
        );

        // charset
        builder.add(
                "name varchar(32) UTF => abc",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32, false, "UTF-8"); } },
                fields("abc"),
                "0x1C00 0000 4142 0200 0100 0000 0001 0000 0004 0361 6263 4241 1C00 0000"
        );
        builder.add(
                "name varchar(32) latin-1 => abc",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32, false, "ISO-8859-1"); } },
                fields("abc"),
                "0x1C00 0000 4142 0200 0100 0000 0001 0000 0004 0361 6263 4241 1C00 0000"
        );
        builder.add(
                "name varchar(32) UTF => cliche",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32, false, "UTF-8"); } },
                fields("cliché"),
                "0x2000 0000 4142 0200 0100 0000 0001 0000 0008 0763 6C69 6368 C3A9 4241 2000 0000"
        );
        builder.add(
                "name varchar(32) latin-1 => cliche",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32, false, "ISO-8859-1"); } },
                fields("cliché"),
                "0x1F00 0000 4142 0200 0100 0000 0001 0000 0007 0663 6C69 6368 E942 411F 0000 00"
        );
        builder.add(
                "name varchar(32) UTF => snowman",
                new TableMaker() { public void make(NewTableBuilder b) { b.colString("name", 32, false, "UTF-8"); } },
                fields("☃"),
                "0x1C00 0000 4142 0200 0100 0000 0001 0000 0004 03E2 9883 4241 1C00 0000"
        );

        return builder.asList();
    }

    @Test
    public void simple() {
        RowData rowData = new RowData(new byte[128]);
        createAndCheck(rowData);
    }

    @Test
    public void withOffset() {
        RowData rowData = new RowData(new byte[128], 64, 128-64);
        createAndCheck(rowData);
    }

    @Test
    public void nonZeroBytes() {
        byte[] bytes = new byte[128];
        Arrays.fill(bytes, (byte)0xFF);
        RowData rowData = new RowData(bytes);
        createAndCheck(rowData);
    }

    public RowDataFormatTest(TableMaker tableMaker, Object[] fields, String bytesString) {
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        NewAISBuilder aisBuilder = AISBBasedBuilder.create(SCHEMA, typesTranslator);
        NewTableBuilder tableBuilder = aisBuilder.table(TABLE).colInt("pkid");
        tableMaker.make(tableBuilder);
        tableBuilder.pk("pkid");
        AkibanInformationSchema ais = aisBuilder.ais();
        Table table = ais.getTable(SCHEMA, TABLE);
        table.setTableId(1);
        new SchemaFactory().buildRowDefs(ais);
        rowDef = ais.getTable(SCHEMA, TABLE).rowDef();
        this.fields = Arrays.copyOf(fields, fields.length);
        this.bytesString = bytesString;
    }

    private final RowDef rowDef;
    private final Object[] fields;
    private final String bytesString;

    private void createAndCheck(RowData rowData) {
        rowData.createRow(rowDef, fields);
        String asHex = Strings.hex(rowData.getBytes(), rowData.getRowStart(), rowData.getRowSize());
        assertEquals("rowData bytes", bytesString.replace(" ", ""), "0x" + asHex);
    }

    private static Object[] fields(Object... args) {
        Object[] ret = new Object[args.length + 1];
        System.arraycopy(args, 0, ret, 1, args.length);
        for (int i=1; i < args.length+1; ++i) {
            if (ret[i] == NULL) {
                ret[i] = null;
            }
        }
        ret[0] = ID;
        return ret;
    }

    private static String str(int length) {
        StringBuilder builder = new StringBuilder();
        for (int i=0; i < length; ++i) {
            builder.append(i % 10);
        }
        return builder.toString();
    }

    private static final String SCHEMA = "rd_format";
    private static final String TABLE = "t1";
    private static final Object NULL = new Object();
    private static final long ID = 1;

    private interface TableMaker {
        public void make(NewTableBuilder builder);
    }
}
