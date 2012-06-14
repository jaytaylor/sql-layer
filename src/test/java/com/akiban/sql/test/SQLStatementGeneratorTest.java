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

package com.akiban.sql.test;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.Locale;

import org.junit.Assert;
import org.junit.Test;

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
