
package com.akiban.server.test.it.keyupdate;

import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.store.TreeRecordVisitor;

import java.util.ArrayList;
import java.util.List;

class RecordCollectingTreeRecordVisistor extends TreeRecordVisitor
{
    @Override
    public void visit(Object[] key, NewRow row)
    {
        records.add(new TreeRecord(key, row));
    }

    public List<TreeRecord> records()
    {
        return records;
    }

    private final List<TreeRecord> records = new ArrayList<>();
}