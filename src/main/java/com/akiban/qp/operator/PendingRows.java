
package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.util.ShareHolder;

import java.util.ArrayList;
import java.util.List;

class PendingRows
{
    public void add(Row row)
    {
        assert !full();
        row(end, row);
        end = next(end);
    }

    public Row take()
    {
        Row row = null;
        if (!empty()) {
            row = row(start);
            row(start, null);
            start = next(start);
        }
        return row;
    }

    public void clear()
    {
        for (int i = 0; i <= capacity; i++) {
            row(i, null);
        }
        start = 0;
        end = 0;
    }

    public boolean isEmpty()
    {
        return start == end;
    }

    public PendingRows(int capacity)
    {
        this.capacity = capacity;
        queue = new ArrayList<>(capacity+1);
        for (int i = 0; i <= capacity; i++) {
            queue.add(new ShareHolder<Row>());
        }
        this.start = 0;
        this.end = 0;
    }

    // For use by this class

    private Row row(int i)
    {
        return queue.get(i).get();
    }

    private void row(int i, Row row)
    {
        queue.get(i).hold(row);
    }

    private boolean empty()
    {
        return end == start;
    }

    private boolean full()
    {
        return next(end) == start;
    }

    private int next(int x)
    {
        if (x++ == capacity) {
            x = 0;
        }
        return x;
    }

    // Object state

    private final int capacity;
    private final List<ShareHolder<Row>> queue;
    private int start;
    private int end;
}
