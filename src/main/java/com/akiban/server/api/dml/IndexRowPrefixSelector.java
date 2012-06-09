package com.akiban.server.api.dml;

// For selecting a prefix of an index row

public class IndexRowPrefixSelector implements ColumnSelector
{
    @Override
    public String toString()
    {
        return String.format("columns(0-%s)", nColumns - 1);
    }

    @Override
    public boolean includesColumn(int columnPosition)
    {
        return columnPosition < nColumns;
    }

    public IndexRowPrefixSelector(int nColumns)
    {
        this.nColumns = nColumns;
    }

    private final int nColumns;
}
