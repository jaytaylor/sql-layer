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

public final class RowDataValueSource extends AbstractRowDataValueSource implements RowDataSource {

    // RowDataSource interface

    @Override
    public void bind(FieldDef fieldDef, RowData rowData) {
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
