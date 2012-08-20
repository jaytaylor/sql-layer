/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.rowdata;

import com.akiban.server.AkServerUtil;
import com.akiban.server.encoding.EncodingException;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.util.ByteSource;

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
        this.adapter = Types3Switch.ON
                ? new NewValueAdapter()
                : new OldValueAdapter();
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

    private static class OldValueAdapter extends ValueAdapter<ValueSource,RowDataValueTarget>{

        @Override
        public void doConvert(ValueSource source, RowDataValueTarget target, FieldDef fieldDef) {
            Converters.convert(source, target);
        }

        @Override
        public void objectToSource(Object object, FieldDef fieldDef) {
            fromObjectValueSource.setReflectively(object);
        }

        @Override
        public ValueSource nullSource(FieldDef fieldDef) {
            return NullValueSource.only();
        }

        @Override
        public boolean isNull(ValueSource source) {
            return source.isNull();
        }

        public OldValueAdapter() {
            this(
                    new FromObjectValueSource(),
                    new RowDataValueTarget());
        }

        private OldValueAdapter(FromObjectValueSource source, RowDataValueTarget target) {
            super(source,  target);
            this.fromObjectValueSource = source;
        }

        private FromObjectValueSource fromObjectValueSource;
    }

    private static class NewValueAdapter extends ValueAdapter<PValueSource,RowDataPValueTarget> {

        @Override
        public void doConvert(PValueSource source, RowDataPValueTarget target, FieldDef fieldDef) {
            TInstance instance = target.targetInstance();
            if (stringInput != null) {
                // turn the string input into a PValueSource, then give it to TClass.fromObject.
                // Strings being inserted to binary types are a special, weird case.
                if (pValue.getUnderlyingType() == PUnderlying.BYTES) {
                    target.putStringBytes(stringInput);
                    return;
                }
                if (stringCache == null)
                    stringCache = new PValue(PUnderlying.STRING);
                stringCache.putString(stringInput, null);
                TExecutionContext context = null;
                instance.typeClass().fromObject(context, stringCache, pValue);
            }
            instance.writeCanonical(pValue, target);
        }

        @Override
        public void objectToSource(Object object, FieldDef fieldDef) {
            PUnderlying underlying = underlying(fieldDef);
            pValue.underlying(underlying);
            stringInput = null;
            if (object == null) {
                PValueTargets.copyFrom(nullSource(fieldDef), pValue);
            }
            else if (object instanceof String) {
                // This is the common case, so let's test for it first
                if (underlying == PUnderlying.STRING)
                    pValue.putString((String)object, null);
                else
                    stringInput = (String)object;
            }
            else {
                switch (underlying) {
                case INT_8:
                case INT_16:
                case UINT_16:
                case INT_32:
                case INT_64:
                    if (object instanceof Number)
                        PValueSources.pvalueFromLong(((Number)object).longValue(), pValue);
                    break;
                case FLOAT:
                    if (object instanceof Number)
                        pValue.putFloat(((Number)object).floatValue());
                    break;
                case DOUBLE:
                    if (object instanceof Number)
                        pValue.putDouble(((Number)object).doubleValue());
                    break;
                case BYTES:
                    if (object instanceof byte[])
                        pValue.putBytes((byte[])object);
                    else if (object instanceof ByteSource)
                        pValue.putBytes(((ByteSource)object).toByteSubarray());
                    break;
                case STRING:
                    assert false : "should have been handled above";
                case BOOL:
                    if (object instanceof Boolean)
                        pValue.putBool((Boolean)object);
                    break;
                }
                if (!pValue.hasAnyValue())
                    pValue.putObject(object); // last ditch effort!
            }
        }

        @Override
        public PValueSource nullSource(FieldDef fieldDef) {
            return PValueSources.getNullSource(underlying(fieldDef));
        }

        @Override
        public boolean isNull(PValueSource source) {
            return (stringInput == null) && source.isNull();
        }

        private PUnderlying underlying(FieldDef fieldDef) {
            return fieldDef.column().tInstance().typeClass().underlyingType();
        }

        public NewValueAdapter() {
            this(
                    new PValue(),
                    new RowDataPValueTarget());
        }

        public NewValueAdapter(PValue pValue, RowDataPValueTarget target) {
            super(pValue,  target);
            this.pValue = pValue;
        }

        private String stringInput;
        private PValue pValue;
        private PValue stringCache;
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
            case 1: return AkServerUtil.getByte(bytes, fixedWidthSectionOffset);
            case 2: return AkServerUtil.getShort(bytes, fixedWidthSectionOffset);
            case 3: return AkServerUtil.getMediumInt(bytes, fixedWidthSectionOffset);
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
