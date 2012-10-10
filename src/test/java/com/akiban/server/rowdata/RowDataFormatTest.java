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

package com.akiban.server.rowdata;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.ais.model.aisb2.NewUserTableBuilder;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.util.Strings;
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
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colLong("id"); } },
                fields(1L),
                "0x1B00 0000 4142 0200 0100 0000 0001 0000 0001 0000 0042 411B 0000 00"
        );
        builder.add(
                "id int => null",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colLong("id"); } },
                fields(NULL),
                "0x1700 0000 4142 0200 0100 0000 0201 0000 0042 4117 0000 00"
        );
        // var length
        builder.add(
                "name varchar(32) => str(1)",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32); } },
                fields(str(1)),
                "0x1A00 0000 4142 0200 0100 0000 0001 0000 0002 0130 4241 1A00 0000"
        );
        builder.add(
                "name varchar(32) => null",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32); } },
                fields(NULL),
                "0x1700 0000 4142 0200 0100 0000 0201 0000 0042 4117 0000 00"
        );
        builder.add(
                "name varchar(32) => str(32)",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32); } },
                fields(str(32)),
                "0x3900 0000 4142 0200 0100 0000 0001 0000 0021 2030 3132 3334 3536 3738 3930 3132 3334 3536 3738 "
                        +"3930 3132 3334 3536 3738 3930 3142 4139 0000 00"
        );
        builder.add(
                "name varchar(1024) => str(1)",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 1024); } },
                fields(str(1)),
                "0x1C00 0000 4142 0200 0100 0000 0001 0000 0003 0001 0030 4241 1C00 0000"
        );
        // mixed
        builder.add(
                "name varchar(32), id int => str(1), 1",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32).colLong("id"); } },
                fields(str(1), 1L),
                "0x1E00 0000 4142 0300 0100 0000 0001 0000 0002 0100 0000 0130 4241 1E00 0000"
        );
        builder.add(
                "id int, name varchar(32) => 1, str(1)",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32).colLong("id"); } },
                fields(str(1), 1L),
                "0x1E00 0000 4142 0300 0100 0000 0001 0000 0002 0100 0000 0130 4241 1E00 0000"
        );
        builder.add(
                "name varchar(32), id int => null, 1",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32).colLong("id"); } },
                fields(NULL, 1L),
                "0x1B00 0000 4142 0300 0100 0000 0201 0000 0001 0000 0042 411B 0000 00"
        );
        builder.add(
                "id int, name varchar(32) => 1, null",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32).colLong("id"); } },
                fields(NULL, 1L),
                "0x1B00 0000 4142 0300 0100 0000 0201 0000 0001 0000 0042 411B 0000 00"
        );
        builder.add(
                "id int, name varchar(32) => null, null",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32).colLong("id"); } },
                fields(NULL, NULL),
                "0x1700 0000 4142 0300 0100 0000 0601 0000 0042 4117 0000 00"
        );

        // charset
        builder.add(
                "name varchar(32) UTF => abc",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32, false, "UTF-8"); } },
                fields("abc"),
                "0x1C00 0000 4142 0200 0100 0000 0001 0000 0004 0361 6263 4241 1C00 0000"
        );
        builder.add(
                "name varchar(32) latin-1 => abc",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32, false, "ISO-8859-1"); } },
                fields("abc"),
                "0x1C00 0000 4142 0200 0100 0000 0001 0000 0004 0361 6263 4241 1C00 0000"
        );
        builder.add(
                "name varchar(32) UTF => cliche",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32, false, "UTF-8"); } },
                fields("cliché"),
                "0x2000 0000 4142 0200 0100 0000 0001 0000 0008 0763 6C69 6368 C3A9 4241 2000 0000"
        );
        builder.add(
                "name varchar(32) latin-1 => cliche",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32, false, "ISO-8859-1"); } },
                fields("cliché"),
                "0x1F00 0000 4142 0200 0100 0000 0001 0000 0007 0663 6C69 6368 E942 411F 0000 00"
        );
        builder.add(
                "name varchar(32) UTF => snowman",
                new TableMaker() { public void make(NewUserTableBuilder b) { b.colString("name", 32, false, "UTF-8"); } },
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
        NewAISBuilder aisBuilder = AISBBasedBuilder.create(SCHEMA);
        NewUserTableBuilder tableBuilder = aisBuilder.userTable(TABLE).colLong("pkid");
        tableMaker.make(tableBuilder);
        tableBuilder.pk("pkid");
        AkibanInformationSchema ais = aisBuilder.ais();
        UserTable table = ais.getUserTable(SCHEMA, TABLE);
        table.setTableId(1);
        RowDefCache rdc = new SchemaFactory().rowDefCache(ais);
        rowDef = rdc.getRowDef(SCHEMA, TABLE);
        this.fields = Arrays.copyOf(fields, fields.length);
        this.bytesString = bytesString;
    }

    private final RowDef rowDef;
    private final Object[] fields;
    private final String bytesString;

    private void createAndCheck(RowData rowData) {
        rowData.createRow(rowDef, fields);
        String asHex = Strings.hex(rowData.getBytes(), rowData.getRowStart(), rowData.getRowSize());
        assertEquals("rowData bytes", bytesString, asHex);
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
        public void make(NewUserTableBuilder builder);
    }
}
