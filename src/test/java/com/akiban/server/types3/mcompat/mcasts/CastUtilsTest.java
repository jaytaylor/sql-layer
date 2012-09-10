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

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.ErrorHandlingMode;
import com.akiban.server.types3.TExecutionContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class CastUtilsTest {

    @Test
    public void truncate1() {
        testTruncate("2.0", "2");
    }

    @Test
    public void truncate2() {
        testTruncate("-2.0", "-2");
    }

    @Test
    public void truncate3() {
        testTruncate("2.b", "2");
    }

    @Test
    public void truncate4() {
        testTruncate("2.4", "2");
    }

    @Test
    public void truncate5() {
        testTruncate("2.5", "3");
    }

    @Test
    public void truncate6() {
        testTruncate("-2.5", "-3");
    }

    private static void testTruncate(String input, String expected) {
        TExecutionContext context = new TExecutionContext(null, null, null, null,
                ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE);
        String actual = CastUtils.truncateNonDigits(input, context);
        assertEquals(input, expected, actual);
    }
}
