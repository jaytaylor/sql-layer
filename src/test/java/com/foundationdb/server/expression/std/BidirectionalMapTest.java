/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.expression.std;

import com.foundationdb.server.types.AkType;
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
