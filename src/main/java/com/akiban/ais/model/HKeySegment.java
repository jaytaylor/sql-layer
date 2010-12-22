package com.akiban.ais.model;

import java.util.ArrayList;
import java.util.List;

public class HKeySegment
{
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

    public HKeyColumn addTableCounter(UserTable table)
    {
        assert table != null;
        HKeyColumn hKeyColumn = new HKeyColumn(this, table);
        columns.add(hKeyColumn);
        return hKeyColumn;
    }

    public HKeySegment()
    {}

    private HKey hKey;
    private UserTable table;
    private List<HKeyColumn> columns = new ArrayList<HKeyColumn>();
    private int positionInHKey;
}
