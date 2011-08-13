/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.rowdata;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.ais.model.aisb2.NewUserTableBuilder;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.util.ArgumentValidation;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Formatter;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class RowDataFormatTest {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();

        // fixed length
        builder.add(
                "id int => 1",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colLong("id"); } },
                fields(1L),
                "0x1B00 0000 4142 0200 9065 0000 0001 0000 0001 0000 0042 411B 0000 00"
        );
        builder.add(
                "id int => null",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colLong("id"); } },
                fields(NULL),
                "0x1700 0000 4142 0200 7869 0000 0201 0000 0042 4117 0000 00"
        );
        // var length
        builder.add(
                "name varchar(32) => str(1)",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colString("name", 32); } },
                fields(str(1)),
                "0x1A00 0000 4142 0200 606D 0000 0001 0000 0002 0130 4241 1A00 0000"
        );
        builder.add(
                "name varchar(32) => null",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colString("name", 32); } },
                fields(NULL),
                "0x1700 0000 4142 0200 4871 0000 0201 0000 0042 4117 0000 00"
        );
        builder.add(
                "name varchar(32) => str(32)",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colString("name", 32); } },
                fields(str(32)),
                "0x3900 0000 4142 0200 3075 0000 0001 0000 0021 2030 3132 3334 3536 3738 3930 3132 3334 3536 3738 "
                        +"3930 3132 3334 3536 3738 3930 3142 4139 0000 00"
        );
        builder.add(
                "name varchar(1024) => str(1)",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colString("name", 1024); } },
                fields(str(1)),
                "0x1C00 0000 4142 0200 1879 0000 0001 0000 0003 0001 0030 4241 1C00 0000"
        );
        // mixed
        builder.add(
                "name varchar(32), id int => str(1), 1",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colString("name", 32).colLong("id"); } },
                fields(str(1), 1L),
                "0x1E00 0000 4142 0300 007D 0000 0001 0000 0002 0100 0000 0130 4241 1E00 0000"
        );
        builder.add(
                "id int, name varchar(32) => 1, str(1)",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colString("name", 32).colLong("id"); } },
                fields(str(1), 1L),
                "0x1E00 0000 4142 0300 E880 0000 0001 0000 0002 0100 0000 0130 4241 1E00 0000"
        );
        builder.add(
                "name varchar(32), id int => null, 1",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colString("name", 32).colLong("id"); } },
                fields(NULL, 1L),
                "0x1B00 0000 4142 0300 D084 0000 0201 0000 0001 0000 0042 411B 0000 00"
        );
        builder.add(
                "id int, name varchar(32) => 1, null",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colString("name", 32).colLong("id"); } },
                fields(NULL, 1L),
                "0x1B00 0000 4142 0300 B888 0000 0201 0000 0001 0000 0042 411B 0000 00"
        );
        builder.add(
                "id int, name varchar(32) => null, null",
                new AISLambda() { public void build(NewUserTableBuilder b) { b.colString("name", 32).colLong("id"); } },
                fields(NULL, NULL),
                "0x1700 0000 4142 0300 A08C 0000 0601 0000 0042 4117 0000 00"
        );

        return builder.asList();
    }

    @Test
    public void checkBytes() {
        RowData rowData = new RowData(new byte[128]);
        rowData.createRow(rowDef, fields);
        String asHex = hex(rowData.getBytes(), rowData.getRowStart(), rowData.getRowSize());
        assertEquals("rowData bytes", bytesString, asHex);
    }

    public RowDataFormatTest(AISLambda aisLambda, Object[] fields, String bytesString) {
        NewAISBuilder aisBuilder = AISBBasedBuilder.create(SCHEMA);
        NewUserTableBuilder tableBuilder = aisBuilder.userTable(TABLE).colLong("pkid");
        aisLambda.build(tableBuilder);
        tableBuilder.pk("pkid");
        AkibanInformationSchema ais = aisBuilder.ais();
        RowDefCache rdc = new SchemaFactory().rowDefCache(ais);
        rowDef = rdc.getRowDef(SCHEMA, TABLE);
        this.fields = Arrays.copyOf(fields, fields.length);
        this.bytesString = bytesString;
    }

    private final RowDef rowDef;
    private final Object[] fields;
    private final String bytesString;

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

    private static String hex(byte[] bytes, int start, int length) {
        ArgumentValidation.isGTE("start", start, 0);
        ArgumentValidation.isGTE("length", length, 0);

        StringBuilder sb = new StringBuilder("0x");
        Formatter formatter = new Formatter(sb, Locale.US);
        for (int i=start; i < start+length; ++i) {
            formatter.format("%02X", bytes[i]);
            if ((i-start) % 2 == 1) {
                sb.append(' ');
            }
        }
        return sb.toString().trim();
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

    private interface AISLambda {
        public void build(NewUserTableBuilder builder);
    }
}
