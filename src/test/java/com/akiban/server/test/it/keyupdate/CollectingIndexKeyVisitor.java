
package com.akiban.server.test.it.keyupdate;

import com.akiban.server.store.IndexKeyVisitor;

import java.util.ArrayList;
import java.util.List;

public class CollectingIndexKeyVisitor extends IndexKeyVisitor
{
    @Override
    protected void visit(List<?> key)
    {
        records.add(key);
    }

    public List<List<?>> records()
    {
        return records;
    }

    private final List<List<?>> records = new ArrayList<>();
}