
package com.akiban.server.api.dml;

public class ByteArrayColumnSelector implements ColumnSelector
{
    @Override
    public boolean includesColumn(int columnPosition)
    {
        return (columnBitMap[columnPosition / 8] & (1 << (columnPosition % 8))) != 0;
    }

    public ByteArrayColumnSelector(byte[] columnBitMap)
    {
        this.columnBitMap = columnBitMap;
    }

    private final byte[] columnBitMap;
}
