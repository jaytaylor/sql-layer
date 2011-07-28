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

package com.akiban.server;

import com.akiban.server.types.AbstractLongSource;
import com.akiban.server.types.AbstractStringSource;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.ConversionDispatch;

public final class FieldDefConversionSource {

    // public interface

    public ConversionSource forField(FieldDef fieldDef, RowData rowData) {
        final FieldDefBinding fieldDefBinding = dispatch.get(fieldDef.getType().akType());
        fieldDefBinding.bind(fieldDef, rowData);
        return fieldDefBinding;
    }

    public FieldDefConversionSource() {}

    // class state
    ConversionDispatch<FieldDefBinding> dispatch = new ConversionDispatch<FieldDefBinding>(
            new FieldDefLongConversion(),
            null,
            new FieldDefStringConversion()
    );

    // for use within this class
    // Stolen from the Encoding classes

    private static long getOffsetAndWidth(FieldDef fieldDef, RowData rowData) {
        return fieldDef.getRowDef().fieldLocation(rowData, fieldDef.getFieldIndex());
    }

    private static long extractLong(FieldDef fieldDef, RowData rowData) {
        long offsetAndWidth = getOffsetAndWidth(fieldDef, rowData);
        assert offsetAndWidth != 0 : fieldDef + " null for " + rowData;
        final int offset = (int)offsetAndWidth;
        final int width = (int)(offsetAndWidth >>> 32);
        return rowData.getIntegerValue(offset, width);
    }

    // nested classes

    private interface FieldDefBinding extends ConversionSource {
        void bind(FieldDef fieldDef, RowData rowData);
    }

    private static class FieldDefLongConversion extends AbstractLongSource implements FieldDefBinding {
        @Override
        protected long computeLong() {
            return extractLong(fieldDef, rowData);
        }

        @Override
        public boolean isNull() {
            return (rowData.isNull(fieldDef.getFieldIndex()));
        }

        @Override
        public void bind(FieldDef fieldDef, RowData rowData) {
            this.fieldDef = fieldDef;
            this.rowData = rowData;
        }

        private FieldDef fieldDef;
        private RowData rowData;
    }

    private static class FieldDefStringConversion extends AbstractStringSource implements FieldDefBinding {
        @Override
        public String computeString() {
            final long location = getOffsetAndWidth(fieldDef, rowData);
            return location == 0
                    ? null
                    : rowData.getStringValue((int) location, (int) (location >>> 32), fieldDef);
        }

        @Override
        public boolean isNull() {
            return (rowData.isNull(fieldDef.getFieldIndex()));
        }

        @Override
        public void bind(FieldDef fieldDef, RowData rowData) {
            this.fieldDef = fieldDef;
            this.rowData = rowData;
        }

        private FieldDef fieldDef;
        private RowData rowData;
    }
}
