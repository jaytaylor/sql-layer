
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

    private final List<ShareHolder<Row>> list = new ArrayList<>(INITIAL_CAPACITY);
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
