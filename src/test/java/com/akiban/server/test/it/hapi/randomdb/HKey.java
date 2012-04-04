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

package com.akiban.server.test.it.hapi.randomdb;

import com.akiban.server.api.dml.scan.NewRow;

import static org.junit.Assert.fail;

class HKey implements Comparable<HKey>
{
    @Override
    public int compareTo(HKey that)
    {
        int n = Math.min(this.key.length, that.key.length);
        for (int i = 0; i < n; i++) {
            long c = this.key[i] - that.key[i];
            if (c != 0) {
                return (int) c;
            }
        }
        return (this.key.length - that.key.length);
    }

    public HKey(RCTortureIT test, NewRow row)
    {
        int tableId = row.getTableId();
        if (tableId == test.customerTable) {
            key = new long[]{
                test.customerTable, (Long) row.get(0)
            };
        } else if (tableId == test.orderTable) {
            key = new long[]{
                test.customerTable, (Long) row.get(0),
                test.orderTable, (Long) row.get(1)
            };
        } else if (tableId == test.itemTable) {
            key = new long[]{
                test.customerTable, (Long) row.get(0),
                test.orderTable, (Long) row.get(1),
                test.itemTable, (Long) row.get(2)
            };
        } else if (tableId == test.addressTable) {
            key = new long[]{
                test.customerTable, (Long) row.get(0),
                test.addressTable, (Long) row.get(1)
            };
        } else {
            key = null;
            fail();
        }
    }

    public int differSegment(HKey that)
    {
        int differSegment;
        int nx = this.key.length;
        int ny = that.key.length;
        int n = Math.min(nx, ny);
        for (differSegment = 0; differSegment < n; differSegment++) {
            if (this.key[differSegment] != that.key[differSegment]) {
                break;
            }
        }
        return differSegment;
    }

    private final long[] key;
}
