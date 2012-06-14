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

package com.akiban.server.api.dml.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.EnumSet;
import java.util.Map;

import org.junit.Test;

public final class SimplePredicateTest {
    @Test
    public void testEquals() throws Exception {
        final SimplePredicate predicate = new SimplePredicate(0, SimplePredicate.Comparison.EQ);
        final Object col0Val = new Object();

        predicate.addColumn(0, col0Val);

        final NewRow start = predicate.getStartRow();
        final NewRow end = predicate.getEndRow();

        assertSame("start and end", start, end);

        Map<Integer,Object> actualFields = start.getFields();
        assertEquals("fields size", 1, actualFields.size());
        assertSame("value[0]", col0Val, actualFields.get(0));

        assertEquals("scan flags",
                EnumSet.of(
                    ScanFlag.START_RANGE_EXCLUSIVE,
                    ScanFlag.END_RANGE_EXCLUSIVE
                ),
                predicate.getScanFlags());
    }
    
    public void testGeneralNE(SimplePredicate.Comparison comparison) {
        final SimplePredicate predicate = new SimplePredicate(0, comparison);
        final Object col0Val = new Object();

        predicate.addColumn(0, col0Val);

        final NewRow start = predicate.getStartRow();
        final NewRow end = predicate.getEndRow();

        EnumSet<ScanFlag> expectedFlags = EnumSet.noneOf(ScanFlag.class);
        final NewRow rowToCheck;
        switch (comparison) {
            case LT:
                expectedFlags.add(ScanFlag.END_RANGE_EXCLUSIVE);
            case LTE:
                expectedFlags.add(ScanFlag.START_AT_BEGINNING);
                assertNull("start isn't null", start);
                assertNotNull("end isn't null", end);
                rowToCheck = end;
                break;
            case GT:
                expectedFlags.add(ScanFlag.START_RANGE_EXCLUSIVE);
            case GTE:
                expectedFlags.add(ScanFlag.END_AT_END);
                assertNotNull("start is null", start);
                assertNull("end isn't null", end);
                rowToCheck = start;
                break;
            default:
                throw new RuntimeException("inapplicable comparison: " + comparison);
        }


        Map<Integer,Object> actualFields = rowToCheck.getFields();
        assertEquals("fields size", 1, actualFields.size());
        assertSame("value[0]", col0Val, actualFields.get(0));

        assertEquals("scan flags", expectedFlags, predicate.getScanFlags());
    }

    @Test
    public void testLT() {
        testGeneralNE(SimplePredicate.Comparison.LT);
    }


    @Test
    public void testLTE() {
        testGeneralNE(SimplePredicate.Comparison.LTE);
    }


    @Test
    public void testGT() {
        testGeneralNE(SimplePredicate.Comparison.GT);
    }


    @Test
    public void testGTE() {
        testGeneralNE(SimplePredicate.Comparison.GTE);
    }

}
