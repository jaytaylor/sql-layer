/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.rowdata;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.AkServerUtil;
import com.foundationdb.server.rowdata.encoding.Encoders;
import com.foundationdb.server.rowdata.encoding.Encoding;

public class FieldDef
{
    private final Column column;

    private final boolean fixedSize;

    private final int maxStorageSize;

    private final int prefixSize;

    private final Encoding encoding;

    private RowDef rowDef;

    private Long typeParameter1;

    private Long typeParameter2;

    public FieldDef(RowDef rowDef, Column column)
    {
        this(column,
             column.fixedSize(),
             column.getMaxStorageSize().intValue(),
             column.getPrefixSize(),
             column.getTypeParameter1(),
             column.getTypeParameter2());
        this.rowDef = rowDef;
    }

    public Column column()
    {
        return column;
    }

    public String getName() {
        return column.getName();
    }

    public Encoding getEncoding() {
        return encoding;
    }

    public int getMaxStorageSize() {
        return maxStorageSize;
    }

    public int getPrefixSize() {
        return prefixSize;
    }

    public boolean isFixedSize() {
        return fixedSize;
    }

    public RowDef getRowDef() {
        return rowDef;
    }

    public Long getTypeParameter1() {
        return typeParameter1;
    }

    public Long getTypeParameter2() {
        return typeParameter2;
    }

    public int getFieldIndex() {
        // setFieldPosition was only done in RowDefBuilder, not in tests that construct FieldDefs directly.
        assert column != null : this;
        return column.getPosition();
    }

    @Override
    public String toString() {
        return getName() + "(" + column.getTypeName() + "(" + getMaxStorageSize() + "))";
    }

    private FieldDef(Column column,
                     boolean fixedSize,
                     int maxStorageSize,
                     int prefixSize,
                     Long typeParameter1,
                     Long typeParameter2) {
        this.column = column;
        this.encoding = Encoders.encodingFor(column.getType());
        this.fixedSize = fixedSize;
        this.maxStorageSize = maxStorageSize;
        this.prefixSize = prefixSize;
        this.typeParameter1 = typeParameter1;
        this.typeParameter2 = typeParameter2;
        this.column.setFieldDef(this);
    }
}
