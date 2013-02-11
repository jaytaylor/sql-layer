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
package com.akiban.direct;

import java.util.HashMap;
import java.util.Map;

import com.akiban.qp.row.Row;

/**
 * TODO - Total hack that this is current static - need to a way to get this
 * into the context for JDBCResultSet.
 * 
 * @author peter
 * 
 */
public class Direct {

    static Map<Integer, Class<? extends DaoPrototype>> classMap = new HashMap<>();
    static ThreadLocal<Map<Integer, DaoPrototype>> instanceMap = new ThreadLocal<Map<Integer, DaoPrototype>>() {

        @Override
        protected Map<Integer, DaoPrototype> initialValue() {
            return new HashMap<Integer, DaoPrototype>();
        }
    };

    /**
     * Register the class of an entity object with a tableId. Used by
     * JDBCResultSet.getEntity().
     * 
     * @param tableId
     * @param c
     */
    public static void registerEntityDaoPrototype(final int tableId, final Class<? extends DaoPrototype> c) {
        classMap.put(tableId, c);
    }

    /**
     * Return a thread-private instance of an entity object of the registered
     * for a given Row, or null if there is none.
     * 
     */
    public static DaoPrototype objectForRow(final Row row) {
        if (row.rowType().hasUserTable()) {
            final int tableId = row.rowType().typeId();
            DaoPrototype o = instanceMap.get().get(tableId);
            if (o != null) {
                return o;
            }
            Class<? extends DaoPrototype> c = classMap.get(tableId);
            if (c != null) {
                try {
                    o = c.newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
            if (o != null) {
                instanceMap.get().put(tableId, o);
            }
            return o;
        } else {
            return null;
        }
    }

}
