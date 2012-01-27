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
