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

import com.akiban.server.types.AkType;

public final class RowDataValueSource extends AbstractRowDataValueSource {

    // FieldDefConversionBase interface

    public void bind(FieldDef fieldDef, RowData rowData) {
        if(fieldDef.getType().akType() == AkType.INTERVAL_MILLIS)
            throw new UnsupportedOperationException();
        this.fieldDef = fieldDef;
        this.rowData = rowData;
    }

    // AbstractRowDataValueSource interface

    @Override
    protected long getRawOffsetAndWidth() {
        return fieldDef().getRowDef().fieldLocation(rowData(), fieldDef().getFieldIndex());
    }

    @Override
    protected byte[] bytes() {
        return rowData.getBytes();
    }

    @Override
    protected FieldDef fieldDef() {
        return fieldDef;
    }

    // ValueSource interface

    @Override
    public boolean isNull() {
        return (rowData().isNull(fieldDef().getFieldIndex()));
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("ValueSource( %s -> %s )", fieldDef, rowData.toString(fieldDef.getRowDef()));
    }

    // private
    
    private RowData rowData() {
        return rowData;
    }

    // object state
    private FieldDef fieldDef;
    private RowData rowData;
}
