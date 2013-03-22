
package com.akiban.server.test.it.keyupdate;

import com.akiban.server.api.dml.scan.NewRow;

public class TreeRecord
{
    @Override
    public int hashCode()
    {
        return hKey.hashCode() ^ row.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        boolean eq = o != null && o instanceof TreeRecord;
        if (eq) {
            TreeRecord that = (TreeRecord) o;
            eq = this.hKey.equals(that.hKey) && equals(this.row, that.row);
        }
        return eq;
    }

    @Override
    public String toString()
    {
        return String.format("%s -> %s", hKey, row);
    }

    public HKey hKey()
    {
        return hKey;
    }

    public NewRow row()
    {
        return row;
    }

    public TreeRecord(HKey hKey, NewRow row)
    {
        this.hKey = hKey;
        this.row = row;
    }

    public TreeRecord(Object[] hKey, NewRow row)
    {
        this(new HKey(hKey), row);
    }

    private boolean equals(NewRow x, NewRow y)
    {
        return
            x == y ||
            x != null && y != null && x.getRowDef() == y.getRowDef() && x.getFields().equals(y.getFields());
    }

    private final HKey hKey;
    private final NewRow row;
}
