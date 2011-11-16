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

package com.akiban.server.expression.std;

import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types.AkType;
import java.util.EnumMap;
import java.util.HashMap;

/**
 * BidirectionalMap: implements a simple bi-directional  map: AkType <--> Integer
 * (To be used in ArithExpression.java)
 */
class BidirectionalMap
{
    private EnumMap<AkType,Integer> map2Int;
    private HashMap<Integer,AkType> map2Ak;

    public BidirectionalMap (int size, float lf)
    {
        map2Int = new EnumMap<AkType,Integer>(AkType.class);
        map2Ak = new HashMap<Integer, AkType>(size, lf);
    }

    public void put (AkType type, int k)
    {
       if (k < 0 ) throw new UnsupportedOperationException("int key has to be non-negative");
       if (get(type) == k && get(k) == type) return;
       if (map2Int.containsKey(type) || map2Ak.containsKey(k)|| map2Int.get(type) != null || map2Ak.get(k) != null)
            throw new  UnsupportedOperationException ("Duplicated key/value");
        map2Int.put(type, k);
        map2Ak.put(k, type);
    }

    /**
     *
     * @param key
     * @return the AkType corresponding to the key, if there is no such thing, null is returned
     */
    public AkType get (int key)
    {
        return map2Ak.get(key);
    }

    /**
     *
     * @param key
     * @return the (integer) key corresponding to AkType, if there is no such thing, -1 is returned
     */
    public int get (AkType key)
    {
        Integer r = map2Int.get(key);
        return r == null ? -1 : r;
    }
}
