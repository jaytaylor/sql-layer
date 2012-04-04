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

package com.akiban.server.test.mt.mthapi.base.sais;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public final class SaisTableTest {
    @Test
    public void countEmptyChildren() {
        SaisTable sole = new SaisBuilder().table("sole", "id").backToBuilder().getSoleRootTable();
        assertEquals("count", 1, sole.countIncludingChildren());
    }

    @Test
    public void countCOIA() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("c", "cid").pk("cid");
        builder.table("o", "oid", "c_id").pk("oid").joinTo("c").col("cid", "c_id");
        builder.table("i", "iid", "o_id").joinTo("o").col("oid", "o_id");
        builder.table("a", "aid", "c_id").pk("aid").joinTo("c").col("cid", "c_id");
        SaisTable customer = builder.getSoleRootTable();
        assertEquals("count", 4, customer.countIncludingChildren());
    }

    @Test
    public void setCOIA() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("c", "cid").pk("cid");
        builder.table("o", "oid", "c_id").pk("oid").joinTo("c").col("cid", "c_id");
        builder.table("i", "iid", "o_id").joinTo("o").col("oid", "o_id");
        builder.table("a", "aid", "c_id").pk("aid").joinTo("c").col("cid", "c_id");
        SaisTable customer = builder.getSoleRootTable();
        SaisTable order = customer.getChild("o");
        SaisTable item = order.getChild("i");
        SaisTable address = customer.getChild("a");

        Set<SaisTable> expectedSet = new HashSet<SaisTable>(Arrays.asList(customer, order, item, address));
        Set<SaisTable> actualSet = customer.setIncludingChildren();

        assertEquals("sets by equality", identityHashMap(expectedSet), identityHashMap(actualSet));
    }
    
    private static IdentityHashMap<SaisTable,String> identityHashMap(Set<SaisTable> tables) {
        IdentityHashMap<SaisTable,String> map = new IdentityHashMap<SaisTable, String>();
        for (SaisTable table : tables) {
            map.put(table, table.getName());
        }
        return map;
    }
}
