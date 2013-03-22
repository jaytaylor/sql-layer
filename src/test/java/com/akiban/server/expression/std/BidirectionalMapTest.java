
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
