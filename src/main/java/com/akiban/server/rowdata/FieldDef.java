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

package com.akiban.server.rowdata;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
import com.akiban.server.AkServerUtil;
import com.akiban.server.encoding.EncoderFactory;
import com.akiban.server.encoding.Encoding;

public class FieldDef {
    private final Column column;

    private final Type type;

    private final String columnName;

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
             column.getType(),
             column.getMaxStorageSize().intValue(),
             column.getPrefixSize(),
             column.getTypeParameter1(),
             column.getTypeParameter2());
        this.rowDef = rowDef;
    }

    public static FieldDef pkLessTableCounter(RowDef rowDef)
    {
        FieldDef fieldDef = new FieldDef(null, null, null, -1, -1, null, null);
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

    public Type getType() {
        return type;
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
        return type.fixedSize();
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
        return columnName + "(" + type + "(" + getMaxStorageSize() + "))";
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
        return type.equals(type) && columnName.equals(def.columnName)
                && encoding == def.encoding && column.getPosition().equals(def.column.getPosition())
                && AkServerUtil.equals(typeParameter1, def.typeParameter1)
                && AkServerUtil.equals(typeParameter2, def.typeParameter2);
    }

    @Override
    public int hashCode() {
        return type.hashCode() ^ columnName.hashCode() ^ encoding.hashCode()
                ^ column.getPosition() ^ AkServerUtil.hashCode(typeParameter1)
                ^ AkServerUtil.hashCode(typeParameter2);
    }

    private FieldDef(Column column,
                     String name,
                     Type type,
                     int maxStorageSize,
                     int prefixSize,
                     Long typeParameter1,
                     Long typeParameter2) {
        this.column = column;
        this.columnName = name;
        this.type = type;
        this.encoding = EncoderFactory.valueOf(type.encoding(), type, column.getCharsetAndCollation().charset());
        this.maxStorageSize = maxStorageSize;
        this.prefixSize = prefixSize;
        this.typeParameter1 = typeParameter1;
        this.typeParameter2 = typeParameter2;
        this.column.setFieldDef(this);
    }
}
