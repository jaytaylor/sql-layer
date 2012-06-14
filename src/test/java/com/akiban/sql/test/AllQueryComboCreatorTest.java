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

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;

public class AllQueryComboCreatorTest {

    @Test
    public void testallCombos() {
        AllQueryComboCreator a = new AllQueryComboCreator();
        String[] in = { "A", "B", "C" };
        HashSet<String> in_list = new HashSet<String>(new HashSet<String>(
                Arrays.asList(in)));
        HashSet<HashSet<String>> out = a.allCombos(in_list);
        System.out.println(out);
        String elements[][] = { { "A", "B", "C" }, { "A", "B" }, { "C", "B" },
                { "A", "C" }, { "A" }, { "B" }, { "C" } };
        for (int x = 0; x < elements.length; x++) {
            Assert.assertTrue("failed " + x, out.contains(new HashSet<String>(
                    Arrays.asList(elements[x]))));
        }
        Assert.assertEquals(out.size(), elements.length);

    }

    @Test
    public void testallCombos2() {
        AllQueryComboCreator a = new AllQueryComboCreator();
        String[] in = { "A", "B", "C", "D" };
        HashSet<String> in_list = new HashSet<String>(Arrays.asList(in));

        long time = System.currentTimeMillis();
        HashSet<HashSet<String>> out = a.allCombos(in_list);
        System.out.println("" + (System.currentTimeMillis() - time) + " ms");

        String elements[][] = { { "A", "B", "C", "D" }, { "A", "B", "C" },
                { "A", "C", "D" }, { "A", "B", "D" }, { "B", "C", "D" },
                { "A", "B" }, { "A", "C" }, { "A", "D" }, { "B", "C" },
                { "B", "D" }, { "C", "D" }, { "A" }, { "B" }, { "C" }, { "D" } };
        for (int x = 0; x < elements.length; x++) {
            Assert.assertTrue("failed " + x, out.contains(new HashSet<String>(
                    Arrays.asList(elements[x]))));
        }
        //System.out.println(out);
        Assert.assertTrue(out.size() >= elements.length);

    }

    @Test
    public void test1() {
        AllQueryComboCreator a = new AllQueryComboCreator();
        // functions need to be reworked
//        Assert.assertEquals("aa = CONCAT('" + a.STR_PARAMS[1] + "','"
//                + a.STR_PARAMS[2] + "')",
//                a.format(1, 0, "aa", a.STR_PARAMS, GenericCreator.STR_METHOD));
//        Assert.assertEquals("ABC = CONCAT('" + a.STR_PARAMS[2] + "','"
//                + a.STR_PARAMS[3] + "')",
//                a.format(2, 0, "ABC", a.STR_PARAMS, GenericCreator.STR_METHOD));

    }

}
