
package com.akiban.ais.model;

import java.util.ArrayList;
import java.util.List;

public class HKeySegment
{
    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(table().getName().getTableName());
        buffer.append(": (");
        boolean firstColumn = true;
        for (HKeyColumn hKeyColumn : columns()) {
            if (firstColumn) {
                firstColumn = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(hKeyColumn.toString());
        }
        buffer.append(")");
        return buffer.toString();
    }

    public HKeySegment(HKey hKey, UserTable table)
    {
        this.hKey = hKey;
        this.table = table;
        if (hKey.segments().isEmpty()) {
            this.positionInHKey = 0;
        } else {
            HKeySegment lastSegment = hKey.segments().get(hKey.segments().size() - 1);
            this.positionInHKey =
                lastSegment.columns().isEmpty()
                ? lastSegment.positionInHKey() + 1
                : lastSegment.columns().get(lastSegment.columns().size() - 1).positionInHKey() + 1;
        }
    }
    
    public HKey hKey()
    {
        return hKey;
    }

    public UserTable table()
    {
        return table;
    }

    public int positionInHKey()
    {
        return positionInHKey;
    }

    public List<HKeyColumn> columns()
    {
        return columns;
    }

    public HKeyColumn addColumn(Column column)
    {
        assert column != null;
        HKeyColumn hKeyColumn = new HKeyColumn(this, column);
        columns.add(hKeyColumn);
        return hKeyColumn;
    }

    private final HKey hKey;
    private final UserTable table;
    private final List<HKeyColumn> columns = new ArrayList<>();
    private final int positionInHKey;
}
