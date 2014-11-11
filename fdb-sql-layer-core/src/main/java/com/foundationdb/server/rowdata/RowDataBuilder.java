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

import com.foundationdb.server.AkServerUtil;
import com.foundationdb.server.rowdata.encoding.EncodingException;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.*;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.util.ByteSource;

public final class RowDataBuilder {

    public void startAllocations() {
        state.require(State.NEWLY_CONSTRUCTED);

        // header
        byte[] bytes = rowData.getBytes();
        AkServerUtil.putShort(bytes, fixedWidthSectionOffset + RowData.O_SIGNATURE_A, RowData.SIGNATURE_A);
        AkServerUtil.putInt(bytes, fixedWidthSectionOffset + RowData.O_ROW_DEF_ID, rowDef.getRowDefId());
        AkServerUtil.putShort(bytes, fixedWidthSectionOffset + RowData.O_FIELD_COUNT, rowDef.getFieldCount());
        fixedWidthSectionOffset = fixedWidthSectionOffset + RowData.O_NULL_MAP;
        nullMapOffset = fixedWidthSectionOffset;
        // TODO unloop this?
        for (int index = 0; index < rowDef.getFieldCount(); index += 8) {
            rowData.getBytes()[fixedWidthSectionOffset++] = 0;
        }

        fieldIndex = 0;
        assert fixedWidthSectionOffset == rowData.getRowStartData()
                : fixedWidthSectionOffset + " != " + rowData.getRowStartData();
        state = State.ALLOCATING;
    }

    public void allocate(FieldDef fieldDef, Object object) {
        checkWithinRange(State.ALLOCATING);

        if (fieldDef != rowDef.getFieldDef(fieldIndex)) {
            throw new IllegalStateException(fieldDef + " but expected " + rowDef.getFieldDef(fieldIndex));
        }
        if (fieldDef.getRowDef() != rowDef) {
            throw new IllegalStateException("expected RowDef " + rowDef + " but found " + fieldDef.getRowDef());
        }

        final int fieldFixedWidth;
        if (fieldDef.isFixedSize()) {
            fieldFixedWidth = (object == null)
                    ? 0
                    : fieldDef.getEncoding().widthFromObject(fieldDef, object);
        } else {
            int fieldMax = fieldDef.getMaxStorageSize();
            vmax += fieldMax;
            if (object == null) {
                fieldFixedWidth = 0;
            }
            else {
                final int varWidth;
                try {
                    varWidth = fieldDef.getEncoding().widthFromObject(fieldDef, object);
                    vlength += varWidth;
                } catch (Exception e) {
                    throw EncodingException.dueTo(e);
                }
                if (varWidth > fieldMax) {
                    throw new EncodingException(
                        String.format("Value for field %s has size %s, exceeding maximum allowed: %s",
                                      fieldDef.column(), varWidth, fieldMax));
                }
                fieldFixedWidth = AkServerUtil.varWidth(vmax);
                byte[] bytes = rowData.getBytes();
                switch (fieldFixedWidth) {
                case 0:
                    break;
                case 1:
                    AkServerUtil.putByte(bytes, fixedWidthSectionOffset, (byte) vlength);
                    break;
                case 2:
                    AkServerUtil.putShort(bytes, fixedWidthSectionOffset, (char) vlength);
                    break;
                case 3:
                    AkServerUtil.putMediumInt(bytes, fixedWidthSectionOffset, vlength);
                    break;
                case 4:
                    AkServerUtil.putInt(bytes, fixedWidthSectionOffset, vlength);
                    break;
                default:
                    throw new UnsupportedOperationException("bad width-of-width: " + fieldFixedWidth);
                }
            }
        }

        fieldWidths[fieldIndex] = fieldFixedWidth;
        fixedWidthSectionOffset += fieldFixedWidth;
        ++fieldIndex;
    }

    public void startPuts() {
        state.require(State.ALLOCATING);
        vlength = 0;
        nullRemainingAllocations();
        variableWidthSectionOffset = fixedWidthSectionOffset;
        // rewind to the start of the fixed length portion
        for (int fieldWidth : fieldWidths) {
            fixedWidthSectionOffset -= fieldWidth;
        }
        fieldIndex = 0;
        state = State.PUTTING;
    }

    public void putObject(Object o) {
        FieldDef fieldDef = rowDef.getFieldDef(fieldIndex);
        adapter.objectToSource(o, fieldDef);
        convert(null, adapter, fieldDef);
    }

    private <S,T extends RowDataTarget> void convert(S source, ValueAdapter<S,T> valueAdapter, FieldDef fieldDef) {
        state.require(State.PUTTING);

        byte[] bytes = rowData.getBytes();
        int currFixedWidth = fieldWidths[fieldIndex];

        if (source == null)
            source = valueAdapter.source();
        T target = valueAdapter.target();

        if (valueAdapter.isNull(source)) {
            if (currFixedWidth != 0) {
                throw new IllegalStateException("expected source to give null: " + source);
            }
            target.bind(fieldDef, bytes, nullMapOffset);
            target.putNull();
            if (target.lastEncodedLength() != 0) {
                throw new IllegalStateException("putting a null should have encoded 0 bytes");
            }
        } else if (fieldDef.isFixedSize()) {
            target.bind(fieldDef, bytes, fixedWidthSectionOffset);
            valueAdapter.convert(fieldDef);
            if (target.lastEncodedLength() != currFixedWidth) {
                throw new IllegalStateException("expected to write " + currFixedWidth
                        + " fixed-width byte(s), but wrote " + target.lastEncodedLength());
            }
        } else {
            target.bind(fieldDef, bytes, variableWidthSectionOffset);
            valueAdapter.convert(fieldDef);
            int varWidthExpected = readVarWidth(bytes, currFixedWidth);
            // the stored value (retrieved by readVarWidth) is actually the *cumulative* length; we want just
            // this field's length. So, we'll subtract from this cumulative value the previously-maintained sum of the
            // previous variable-length fields, and use that for our comparison. Once that's done, we'll add this
            // field's length to that cumulative sum.
            varWidthExpected -= vlength;
            if (target.lastEncodedLength() != varWidthExpected) {
                throw new IllegalStateException("expected to write " + varWidthExpected
                        + " variable-width byte(s), but wrote " + target.lastEncodedLength()
                        + " (vlength=" + vlength + "). FieldDef=" + fieldDef + ", value = <" + source + '>');
            }
            vlength += varWidthExpected;
            variableWidthSectionOffset += varWidthExpected;
        }
        fixedWidthSectionOffset += currFixedWidth;

        ++fieldIndex;
    }

    public int finalOffset() {
        state.require(State.PUTTING);
        nullRemainingPuts(adapter);

        // footer
        byte[] bytes = rowData.getBytes();
        AkServerUtil.putShort(bytes, variableWidthSectionOffset, RowData.SIGNATURE_B);
        variableWidthSectionOffset += 6;
        int length = variableWidthSectionOffset - rowData.getRowStart();
        AkServerUtil.putInt(bytes, rowData.getRowStart() + RowData.O_LENGTH_A, length);
        AkServerUtil.putInt(bytes, variableWidthSectionOffset + RowData.O_LENGTH_B, length);

        state = State.DONE;
        return variableWidthSectionOffset;
    }

    public RowDataBuilder(RowDef rowDef, RowData rowData) {
        // TODO argument validations
        this.rowDef = rowDef;
        this.rowData = rowData;
        fixedWidthSectionOffset = rowData.getRowStart();
        state = State.NEWLY_CONSTRUCTED;
        this.fieldWidths = new int[rowDef.getFieldCount()];
        this.adapter = new NewValueAdapter();
    }

    private final ValueAdapter<?,?> adapter;
    private final RowDef rowDef;
    private final RowData rowData;
    private final int[] fieldWidths;
    private State state;
    private int fixedWidthSectionOffset;
    private int fieldIndex;
    private int nullMapOffset = -1;
    private int vmax;
    private int vlength;
    private int variableWidthSectionOffset;

    private void checkWithinRange(State requiredState) {
        state.require(requiredState);
        assert fieldIndex >= 0;
        if (fieldIndex >= rowDef.getFieldCount()) {
            throw new IllegalArgumentException("went past last field index of " + rowDef.getFieldCount());
        }
    }

    private static abstract class ValueAdapter<S,T extends RowDataTarget> {
        public abstract void doConvert(S source, T target, FieldDef fieldDef);
        public abstract void objectToSource(Object object, FieldDef fieldDef);
        protected abstract S nullSource(FieldDef fieldDef);
        public abstract boolean isNull(S source);

        public void convert(FieldDef fieldDef) {
            try {
                doConvert(source, target, fieldDef);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw EncodingException.dueTo(e); // assumed to be during writing to the RowData's byte[]
            }
        }

        public S source() {
            return source;
        }

        public T target() {
            return target;
        }


        protected ValueAdapter(S source, T target) {
            this.source = source;
            this.target = target;
        }

        private S source;
        private T target;
    }

    private static class NewValueAdapter extends ValueAdapter<ValueSource,RowDataValueTarget> {

        @Override
        public void doConvert(ValueSource source, RowDataValueTarget target, FieldDef fieldDef) {
            TInstance type = target.targetType();
            if (stringInput != null) {
                // turn the string input into a ValueSource, then give it to TClass.fromObject.
                // Strings being inserted to binary types are a special, weird case.
                if (type.typeClass() instanceof MBinary) {
                    target.putStringBytes(stringInput);
                    return;
                }
                if (stringCache == null)
                    stringCache = new Value(MString.VARCHAR.instance(Integer.MAX_VALUE, true));
                stringCache.putString(stringInput, null);
                TExecutionContext context = new TExecutionContext(null, type, null);
                type.typeClass().fromObject(context, stringCache, value);
            }
            type.writeCanonical(value, target);
        }

        @Override
        public void objectToSource(Object object, FieldDef fieldDef) {
            TInstance underlying = underlying(fieldDef);
            value.underlying(underlying);
            stringInput = null;
            if (object == null) {
                ValueTargets.copyFrom(nullSource(fieldDef), value);
            }
            else if (object instanceof String) {
                // This is the common case, so let's test for it first
                if (TInstance.underlyingType(underlying) == UnderlyingType.STRING)
                    value.putString((String)object, null);
                else
                    stringInput = (String)object;
            }
            else {
                switch (TInstance.underlyingType(underlying)) {
                case INT_8:
                case INT_16:
                case UINT_16:
                case INT_32:
                case INT_64:
                    if (object instanceof Number)
                        ValueSources.valueFromLong(((Number) object).longValue(), value);
                    break;
                case FLOAT:
                    if (object instanceof Number)
                        value.putFloat(((Number)object).floatValue());
                    break;
                case DOUBLE:
                    if (object instanceof Number)
                        value.putDouble(((Number)object).doubleValue());
                    break;
                case BYTES:
                    if (object instanceof byte[])
                        value.putBytes((byte[])object);
                    else if (object instanceof ByteSource)
                        value.putBytes(((ByteSource)object).toByteSubarray());
                    break;
                case STRING:
                    value.putString(object.toString(), null);
                    break;
                case BOOL:
                    if (object instanceof Boolean)
                        value.putBool((Boolean)object);
                    break;
                }
                if (!value.hasAnyValue()) {
                    // last ditch effort! This is mostly to play nice with the loosey-goosy typing from types2.
                    ValueCacher cacher = fieldDef.column().getType().typeClass().cacher();
                    if (cacher != null)
                        object = cacher.sanitize(object);
                    value.putObject(object);
                }
            }
        }

        @Override
        public ValueSource nullSource(FieldDef fieldDef) {
            return ValueSources.getNullSource(underlying(fieldDef));
        }

        @Override
        public boolean isNull(ValueSource source) {
            return (stringInput == null) && source.isNull();
        }

        private TInstance underlying(FieldDef fieldDef) {
            return fieldDef.column().getType();
        }

        public NewValueAdapter() {
            this(
                    new Value(),
                    new RowDataValueTarget());
        }

        public NewValueAdapter(Value value, RowDataValueTarget target) {
            super(value,  target);
            this.value = value;
        }

        private String stringInput;
        private Value value;
        private Value stringCache;
    }

    private void nullRemainingAllocations() {
        int fieldsCount = rowDef.getFieldCount();
        while ( fieldIndex < fieldsCount) {
            allocate(rowDef.getFieldDef(fieldIndex), null);
        }
    }

    private <S> void nullRemainingPuts(ValueAdapter<S,?> adapter) {
        int fieldsCount = rowDef.getFieldCount();
        while ( fieldIndex < fieldsCount) {
            FieldDef fieldDef = rowDef.getFieldDef(fieldIndex);
            S source = adapter.nullSource(fieldDef);
            convert(source, adapter, fieldDef);
        }
    }

    private int readVarWidth(byte[] bytes, int widthWidth) {
        switch (widthWidth) {
            case 1: return AkServerUtil.getUByte(bytes, fixedWidthSectionOffset);
            case 2: return AkServerUtil.getUShort(bytes, fixedWidthSectionOffset);
            case 3: return AkServerUtil.getUMediumInt(bytes, fixedWidthSectionOffset);
            case 4: return AkServerUtil.getInt(bytes, fixedWidthSectionOffset);
            default: throw new UnsupportedOperationException("bad width-of-width: " + widthWidth);
        }
    }

    private enum State {
        NEWLY_CONSTRUCTED,
        ALLOCATING,
        PUTTING,
        DONE
        ;

        void require(State requiredState) {
            if (this != requiredState) {
                throw new IllegalStateException("required state " + requiredState + " but am currently " + name());
            }
        }
    }
}
