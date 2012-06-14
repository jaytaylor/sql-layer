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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.util.ShareHolder;

import java.util.ArrayList;
import java.util.List;

class RowList
{
    public void add(Row row)
    {
        ensureRoom();
        list.get(count++).hold(row);
    }

    public Scan scan()
    {
        scan.reset();
        return scan;
    }

    public void clear()
    {
        for (ShareHolder<Row> rowHolder : list) {
            rowHolder.release();
        }
        count = 0;
    }

    public RowList()
    {
        for (int i = 0; i < INITIAL_CAPACITY; i++) {
            list.add(new ShareHolder<Row>());
        }
    }

    // For use by this class

    private void ensureRoom()
    {
        assert count <= capacity;
        if (count == capacity) {
            int newCapacity = capacity * 2;
            for (int i = capacity; i < newCapacity; i++) {
                list.add(new ShareHolder<Row>());
            }
            capacity = newCapacity;
        }
        assert count < capacity;
    }

    // Class state

    private static final int INITIAL_CAPACITY = 10;

    // Object state

    private final List<ShareHolder<Row>> list = new ArrayList<ShareHolder<Row>>(INITIAL_CAPACITY);
    private int count = 0;
    private int capacity = INITIAL_CAPACITY;
    private final Scan scan = new Scan(); // There should be only one scan of a RowList at a time.

    // Inner classes

    public class Scan
    {
        public Row next()
        {
            Row next = null;
            if (position < count) {
                next = list.get(position++).get();
            }
            return next;
        }

        void reset()
        {
            position = 0;
        }

        private int position = 0;
    }
}
