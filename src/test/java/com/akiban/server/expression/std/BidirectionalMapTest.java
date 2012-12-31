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

package com.akiban.server.expression.std;

import com.akiban.server.types.AkType;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class BidirectionalMapTest
{
    private static BidirectionalMap map = new BidirectionalMap(12, 0.5f);

    @Before
    public void initMap ()
    {
        map.put(AkType.DATE, 1);
        map.put(AkType.DATETIME, 3);
        map.put(AkType.TIME, 5);
    }

    @Test
    public void testPutGet ()
    {
        assertTrue (map.get(1) == map.get(map.get(AkType.DATE)));
        assertTrue (map.get(3) == AkType.DATETIME);
        assertTrue (map.get(6) == null);
        assertTrue (map.get(AkType.LONG) == -1);
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testPutNegIntKey ()
    {
        map.put(AkType.DOUBLE, -1);
    }

    @Test (expected = UnsupportedOperationException.class) // expect dupplicated key
    public void testPutDuplicateAkType ()
    {
        map.put(AkType.DATE, 9);
    }

    @Test (expected = UnsupportedOperationException.class) // expect dupplicated key
    public void testPutDuplicateInt ()
    {
        map.put(AkType.LONG, 5);
    }

    @Test (expected = UnsupportedOperationException.class) // expect dupplicated key
    public void testPutDuplicateBoth ()
    {
        map.put(AkType.DATE, 5);
    }

    @Test
    public void testAcceptableDuplicate () // do not expect duplicated key exception
    {
        map.put(AkType.DATE, 1);
    }

}
