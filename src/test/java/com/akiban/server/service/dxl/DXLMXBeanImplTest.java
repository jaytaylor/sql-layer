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

package com.akiban.server.service.dxl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.util.AssertUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class DXLMXBeanImplTest {

    @Test
    public void giDDL_normal() {
        AkibanInformationSchema ais = AISBBasedBuilder.create("schema1")
                .userTable("customers").colLong("cid").colString("name", 32).pk("cid")
                .userTable("orders").colLong("oid").colLong("cid").colString("odate", 32).pk("oid")
                .joinTo("customers").on("cid", "cid")
                .userTable("addresses").colLong("aid").colLong("cid").colString("street", 32).pk("aid")
                .joinTo("customers").on("cid", "cid")
                .userTable("s2", "page").colLong("pid").colString("title", 64).pk("pid")
                .userTable("s2", "comment").colLong("mid").colLong("pid").colString("content", 128).pk("mid")
                .joinTo("s2", "page").on("pid", "pid")
                .groupIndex("gi1", Index.JoinType.LEFT).on("customers", "name").and("orders", "odate")
                .groupIndex("gi2", Index.JoinType.RIGHT).on("addresses", "street").and("customers", "name")
                .groupIndex("gi3", Index.JoinType.RIGHT).on("s2", "page", "title").and("s2", "comment", "content")
                .ais();
        checkGiDDLs(ais, "schema1",
                "CREATE INDEX gi1 ON orders ( customers.name , orders.odate ) USING LEFT JOIN",
                "CREATE INDEX gi2 ON addresses ( addresses.street , customers.name ) USING RIGHT JOIN",
                "CREATE INDEX gi3 ON s2.comment ( page.title , comment.content ) USING RIGHT JOIN"
        );
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void giDDL_multiSchemaGroup() {
        AkibanInformationSchema ais;
        try {
            ais = AISBBasedBuilder.create("UNUSED")
                    .userTable("s1", "customers").colLong("cid").colString("name", 32).pk("cid")
                    .userTable("s2", "orders").colLong("oid").colLong("cid").colString("odate", 32).pk("oid")
                    .joinTo("s1", "customers").on("cid", "cid")
                    .groupIndex("gi1", Index.JoinType.LEFT).on("s1", "customers", "name").and("s2", "orders", "odate")
                    .ais();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DXLMXBeanImpl.listGiDDLs(ais, "s1");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void escapedSchemaName() {
        AkibanInformationSchema ais;
        try {
            ais = AISBBasedBuilder.create("☃")
                    .userTable("customers").colLong("cid").colString("name", 32).pk("cid")
                    .userTable("orders").colLong("oid").colLong("cid").colString("odate", 32).pk("oid")
                    .joinTo("customers").on("cid", "cid")
                    .groupIndex("gi1", Index.JoinType.LEFT).on("customers", "name").and("orders", "odate")
                    .ais();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DXLMXBeanImpl.listGiDDLs(ais, "s1");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void escapedTableName() {
        AkibanInformationSchema ais;
        try {
            ais = AISBBasedBuilder.create("s1")
                    .userTable("customers").colLong("cid").colString("name", 32).pk("cid")
                    .userTable("☃").colLong("oid").colLong("cid").colString("odate", 32).pk("oid")
                    .joinTo("customers").on("cid", "cid")
                    .groupIndex("gi1", Index.JoinType.LEFT).on("customers", "name").and("☃", "odate")
                    .ais();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DXLMXBeanImpl.listGiDDLs(ais, "s1");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void escapedColumnName() {
        AkibanInformationSchema ais;
        try {
            ais = AISBBasedBuilder.create("s1")
                    .userTable("customers").colLong("cid").colString("name", 32).pk("cid")
                    .userTable("orders").colLong("oid").colLong("cid").colString("☃", 32).pk("oid")
                    .joinTo("customers").on("cid", "cid")
                    .groupIndex("gi1", Index.JoinType.LEFT).on("customers", "name").and("orders", "☃")
                    .ais();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DXLMXBeanImpl.listGiDDLs(ais, "s1");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void escapedColumnName_leadingSpace() {
        AkibanInformationSchema ais;
        try {
            ais = AISBBasedBuilder.create("s1")
                    .userTable("customers").colLong("cid").colString("name", 32).pk("cid")
                    .userTable("orders").colLong("oid").colLong("cid").colString(" odate", 32).pk("oid")
                    .joinTo("customers").on("cid", "cid")
                    .groupIndex("gi1", Index.JoinType.LEFT).on("customers", "name").and("orders", " odate")
                    .ais();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DXLMXBeanImpl.listGiDDLs(ais, "s1");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void escapedColumnName_leadingDigit() {
        AkibanInformationSchema ais;
        try {
            ais = AISBBasedBuilder.create("s1")
                    .userTable("customers").colLong("cid").colString("name", 32).pk("cid")
                    .userTable("orders").colLong("oid").colLong("cid").colString("2odate", 32).pk("oid")
                    .joinTo("customers").on("cid", "cid")
                    .groupIndex("gi1", Index.JoinType.LEFT).on("customers", "name").and("orders", "2odate")
                    .ais();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DXLMXBeanImpl.listGiDDLs(ais, "s1");
    }

    private void checkGiDDLs(AkibanInformationSchema ais, String usingSchema, String... expectedDDLs) {
        List<String> expectedList = Arrays.asList(expectedDDLs);
        List<String> actualList = new ArrayList<String>(DXLMXBeanImpl.listGiDDLs(ais, usingSchema));
        // order is undefined
        Collections.sort(expectedList);
        Collections.sort(actualList);
        AssertUtils.assertCollectionEquals("GI DDLs", expectedList, actualList);
    }
}
