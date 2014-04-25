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

public class FieldDef {
    private final Column column;

    private final String columnName;

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
             column.getName(),
             column.fixedSize(),
             column.getMaxStorageSize().intValue(),
             column.getPrefixSize(),
             column.getTypeParameter1(),
             column.getTypeParameter2());
        this.rowDef = rowDef;
    }

    public static FieldDef pkLessTableCounter(RowDef rowDef)
    {
        FieldDef fieldDef = new FieldDef(null, null, false, -1, -1, null, null);
        fieldDef.rowDef = rowDef;
        return fieldDef;
    }

    public boolean isPKLessTableCounter()
    {
        return rowDef != null && column == null;
    }

    public Column column()
    {
        return column;
    }

    public String getName() {
        return columnName;
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

    public void setRowDef(RowDef parent) {
        this.rowDef = parent;
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
        // setFieldPosition was only done in RowDefCache, not in tests that construct FieldDefs directly.
        assert column != null : this;
        return column.getPosition();
    }

    @Override
    public String toString() {
        return columnName + "(" + column.getTypeName() + "(" + getMaxStorageSize() + "))";
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (!o.getClass().equals(FieldDef.class)) {
            return false;
        }
        FieldDef def = (FieldDef) o;
        return columnName.equals(def.columnName)
                && encoding == def.encoding && column.getPosition().equals(def.column.getPosition())
                && AkServerUtil.equals(typeParameter1, def.typeParameter1)
                && AkServerUtil.equals(typeParameter2, def.typeParameter2);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode() ^ encoding.hashCode()
                ^ column.getPosition() ^ AkServerUtil.hashCode(typeParameter1)
                ^ AkServerUtil.hashCode(typeParameter2);
    }

    private FieldDef(Column column,
                     String name,
                     boolean fixedSize,
                     int maxStorageSize,
                     int prefixSize,
                     Long typeParameter1,
                     Long typeParameter2) {
        this.column = column;
        this.columnName = name;
        this.encoding = Encoders.encodingFor(column.getType());
        this.fixedSize = fixedSize;
        this.maxStorageSize = maxStorageSize;
        this.prefixSize = prefixSize;
        this.typeParameter1 = typeParameter1;
        this.typeParameter2 = typeParameter2;
        this.column.setFieldDef(this);
    }
}
