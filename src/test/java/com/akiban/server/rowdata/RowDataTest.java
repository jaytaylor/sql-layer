
package com.akiban.server.rowdata;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.google.common.base.Strings;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class RowDataTest {
    @Test
    public void reallocateOnBind() throws ClassNotFoundException {
        AkibanInformationSchema ais = AISBBasedBuilder.create()
                .userTable("myschema", "mytable")
                .colLong("id", false)
                .colLong("int_0", true)
                .colLong("int_1", true)
                .colLong("int_2", true)
                .colLong("int_3", true)
                .colLong("int_4", true)
                .colString("bigstring", 500)
                .colString("smallstring", 2)
                .ais();

        List<?> values = Arrays.asList(
                1L, // id
                null, null, null, null, null, // int_x's
                Strings.repeat("a", 476), // bigstring
                "hi" // smallstring
        );
        Object[] valuesArray = values.toArray(new Object[values.size()]);
        RowData rowData = new RowData(new byte[500]);

        new SchemaFactory().buildRowDefs(ais);
        RowDef rowDef = ais.getTable("myschema", "mytable").rowDef();
        assertNotNull("RowDef", rowDef);

        try {
            rowData.createRow(rowDef, valuesArray, false);
            fail("expected ArrayIndexOutOfBoundsException");
        } catch (ArrayIndexOutOfBoundsException e) {
            boolean foundInBind = false;
            for (StackTraceElement frame : e.getStackTrace()) {
                Class<?> frameClass = Class.forName(frame.getClassName());
                if (RowDataTarget.class.isAssignableFrom(frameClass) && "bind".equals(frame.getMethodName())) {
                    foundInBind = true;
                    break;
                }
            }
            assertTrue("stack trace didn't include RowDataTarget.bind", foundInBind);
        }
    }

    @Test
    public void unsignedWidth() throws ClassNotFoundException {
        AkibanInformationSchema ais = AISBBasedBuilder.create()
                .userTable("myschema", "mytable2")
                .colLong("id", false)
                .colString("smallstring", 129)
                .colString("bigstring", 32769)
                .ais();

        new SchemaFactory().buildRowDefs(ais);
        RowDef rowDef = ais.getTable("myschema", "mytable2").rowDef();
        assertNotNull("RowDef", rowDef);

        for (int pass = 1; pass <= 2; pass++) {
            List<?> values = Arrays.asList((long)pass,
                                           (pass == 1) ? Strings.repeat("x", 128) : null,
                                           (pass == 2) ? Strings.repeat("z", 32768) : null);
            Object[] valuesArray = values.toArray(new Object[values.size()]);
            RowData rowData = new RowData(new byte[500]);

            rowData.createRow(rowDef, valuesArray, true);
            assertEquals(String.format("mytable2(%d,%s,%s,)",
                                       valuesArray[0],
                                       (valuesArray[1] != null) ? valuesArray[1] : "",
                                       (valuesArray[2] != null) ? valuesArray[2] : ""),
                         rowData.toString());
        }
    }

}
