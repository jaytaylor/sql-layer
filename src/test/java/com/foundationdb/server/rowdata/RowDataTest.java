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
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.google.common.base.Strings;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class RowDataTest {
    private final TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;

    @Test
    public void reallocateOnBind() throws ClassNotFoundException {
        AkibanInformationSchema ais = AISBBasedBuilder.create(typesTranslator)
                .table("myschema", "mytable")
                .colInt("id", false)
                .colInt("int_0", true)
                .colInt("int_1", true)
                .colInt("int_2", true)
                .colInt("int_3", true)
                .colInt("int_4", true)
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
        AkibanInformationSchema ais = AISBBasedBuilder.create(typesTranslator)
                .table("myschema", "mytable2")
                .colInt("id", false)
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
