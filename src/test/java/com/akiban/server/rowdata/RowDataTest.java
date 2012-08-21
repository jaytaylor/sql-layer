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
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.server.encoding.EncodingException;
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

        RowDefCache rdc = new SchemaFactory().rowDefCache(ais);
        RowDef rowDef = rdc.getRowDef("myschema", "mytable");
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

        RowDefCache rdc = new SchemaFactory().rowDefCache(ais);
        RowDef rowDef = rdc.getRowDef("myschema", "mytable2");
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
