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
