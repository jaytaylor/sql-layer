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

package com.akiban.sql.test;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.Locale;

import org.junit.Assert;
import org.junit.Test;

import com.akiban.sql.test.GenericCreator.Relationship;

public class SQLStatementGeneratorTest {

    @Test
    public void testExtractFromClause() {
        SQLStatementGenerator a = new SQLStatementGenerator();
        
        assertStringEQ("[test.customers]", a.extractTableList(" test.customers.name ").toString());
        
        assertStringEQ("[test.items]".toUpperCase(), a.extractTableList(" test.items.name ").toString().trim());
        assertStringEQ("[customers]",
                a.extractTableList(" customers.name, customers.AAA ").toString());
        ArrayList<String> test = new ArrayList<String>();
        test.add("customers".toUpperCase());
        test.add("orders".toUpperCase());
        test.add("items".toUpperCase());
        Assert.assertEquals(test,
                a.extractTableList(" customers.name, orders.AAA, customers.AAA, orders.SSSS, items.AAA, customers.GGGG "));
        assertStringEQ("[customers]",
                a.extractTableList(" customers.name ").toString());
        assertStringEQ("[customers]",
                a.extractTableList(" customers.name ").toString());
        assertStringEQ("[]", a.extractTableList("  ").toString());
        assertStringEQ("[]", a.extractTableList("").toString());
        assertStringEQ("[customers]",
                a.extractTableList("customers.customer_id").toString());
        
        test = new ArrayList<String>();
        test.add("orders".toUpperCase());
        test.add("addresses".toUpperCase());
        test.add("customers".toUpperCase());
        
        
        Assert.assertEquals(test,
                a.extractTableList("orders.order_date,orders.customer_id,addresses.state,orders.ship_date,customers.customer_id,customers.customer_title,addresses.customer_id"));
         
    }

    private void assertStringEQ(String expected, String actual) {
        Assert.assertEquals(expected.trim().toUpperCase(), actual.trim().toUpperCase());
    }

    @Test
    public void testfixFrom() {
        SQLStatementGenerator a = new SQLStatementGenerator();
        a.setTableList("test.a.a,test.b.a,test.c.a,test.d.a,test.e.a,test.f.a,test.g.a,test.h.a,test.i.a");
        a.fixFrom("test.b");
        Assert.assertEquals("[TEST.A, TEST.I, TEST.C, TEST.D, TEST.E, TEST.F, TEST.G, TEST.H, TEST.B]", a.getTableList().toString());
    }
    
    @Test
    public void test2() {
        SQLStatementGenerator stmt = new SQLStatementGenerator();
        
        stmt.getTableList().add(AllQueryComboCreator.RELATIONSHIPS[1].primaryTable.toUpperCase());
        stmt.getTableList().add(AllQueryComboCreator.RELATIONSHIPS[1].secondaryTable.toUpperCase());
        stmt.getTableList().add("TEST.ORDERS");
        stmt.getTableList().add("TEST.ITEMS");
        int count = stmt.getTableList().size();
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb , Locale.US);
        formatter.format(AllQueryComboCreator.JOIN_OTHER[0],
                AllQueryComboCreator.RELATIONSHIPS[1].primaryTable.toUpperCase(),
                AllQueryComboCreator.RELATIONSHIPS[1].primaryKey.toUpperCase(),
                AllQueryComboCreator.RELATIONSHIPS[1].secondaryTable.toUpperCase(),
                AllQueryComboCreator.RELATIONSHIPS[1].secondaryKey.toUpperCase());

        stmt.getTableList().remove(AllQueryComboCreator.RELATIONSHIPS[1].secondaryTable.toUpperCase());
        stmt.setJoins(1, sb.toString());
        stmt.fixFrom(AllQueryComboCreator.RELATIONSHIPS[1].primaryTable.toUpperCase());
        Assert.assertEquals(count-1, stmt.getTableList().size());
        
    }
}
