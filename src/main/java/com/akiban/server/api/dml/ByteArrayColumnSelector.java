/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

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
