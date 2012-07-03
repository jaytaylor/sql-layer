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
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSources.ValueSourceConverter;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

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
        if (source == null) {
            source = new FromObjectValueSource();
        }
        source.setReflectively(o);
        convert(source);
    }

    public void convert(ValueSource source) {
        state.require(State.PUTTING);

        FieldDef fieldDef = rowDef.getFieldDef(fieldIndex);
        byte[] bytes = rowData.getBytes();
        int currFixedWidth = fieldWidths[fieldIndex];
        if (Types3Switch.ON) {
            if (source.isNull()) {
                if (currFixedWidth != 0) {
                    throw new IllegalStateException("expected source to give null: " + source);
                }
                pTarget.bind(fieldDef, bytes, nullMapOffset);
                pTarget.putNull();
                if (pTarget.lastEncodedLength() != 0) {
                    throw new IllegalStateException("putting a null should have encoded 0 bytes");
                }
            } else if (fieldDef.isFixedSize()) {
                pTarget.bind(fieldDef, bytes, fixedWidthSectionOffset);
                converter.convert(rowDef.getFieldDef(fieldIndex), source, pTarget);
                if (pTarget.lastEncodedLength() != currFixedWidth) {
                    throw new IllegalStateException("expected to write " + currFixedWidth
                            + " fixed-width byte(s), but wrote " + pTarget.lastEncodedLength());
                }
            } else {
                pTarget.bind(fieldDef, bytes, variableWidthSectionOffset);
                converter.convert(rowDef.getFieldDef(fieldIndex), source, pTarget);
                int varWidthExpected = readVarWidth(bytes, currFixedWidth);
                // the stored value (retrieved by readVarWidth) is actually the *cumulative* length; we want just
                // this field's length. So, we'll subtract from this cumulative value the previously-maintained sum of the
                // previous variable-length fields, and use that for our comparison. Once that's done, we'll add this
                // field's length to that cumulative sum.
                varWidthExpected -= vlength;
                if (pTarget.lastEncodedLength() != varWidthExpected) {
                    throw new IllegalStateException("expected to write " + varWidthExpected
                            + " variable-width byte(s), but wrote " + pTarget.lastEncodedLength()
                            + " (vlength=" + vlength + ')');
                }
                vlength += varWidthExpected;
                variableWidthSectionOffset += varWidthExpected;
            }
        } else {
            if (source.isNull()) {
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
                doConvert(source);
                if (target.lastEncodedLength() != currFixedWidth) {
                    throw new IllegalStateException("expected to write " + currFixedWidth
                            + " fixed-width byte(s), but wrote " + target.lastEncodedLength());
                }
            } else {
                target.bind(fieldDef, bytes, variableWidthSectionOffset);
                doConvert(source);
                int varWidthExpected = readVarWidth(bytes, currFixedWidth);
                // the stored value (retrieved by readVarWidth) is actually the *cumulative* length; we want just
                // this field's length. So, we'll subtract from this cumulative value the previously-maintained sum of the
                // previous variable-length fields, and use that for our comparison. Once that's done, we'll add this
                // field's length to that cumulative sum.
                varWidthExpected -= vlength;
                if (target.lastEncodedLength() != varWidthExpected) {
                    throw new IllegalStateException("expected to write " + varWidthExpected
                            + " variable-width byte(s), but wrote " + target.lastEncodedLength()
                            + " (vlength=" + vlength + ')');
                }
                vlength += varWidthExpected;
                variableWidthSectionOffset += varWidthExpected;
            }
        }
        fixedWidthSectionOffset += currFixedWidth;

        ++fieldIndex;
    }

    public int finalOffset() {
        state.require(State.PUTTING);
        nullRemainingPuts();

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
    }

    private final RowDataValueTarget target = new RowDataValueTarget();
    private final RowDataPValueTarget pTarget = new RowDataPValueTarget();
    private FromObjectValueSource source = null; // lazy-loaded
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

    private void doConvert(ValueSource source) {
        try {
            Converters.convert(source, target);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw EncodingException.dueTo(e); // assumed to be during writing to the RowData's byte[]
        }
    }

    private void nullRemainingAllocations() {
        int fieldsCount = rowDef.getFieldCount();
        while ( fieldIndex < fieldsCount) {
            allocate(rowDef.getFieldDef(fieldIndex), null);
        }
    }

    private void nullRemainingPuts() {
        int fieldsCount = rowDef.getFieldCount();
        while ( fieldIndex < fieldsCount) {
            convert(NullValueSource.only());
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

    private static final ValueSourceConverter<FieldDef> converter = new ValueSourceConverter<FieldDef>() {

        @Override
        protected ValueSource tweakSource(FieldDef fieldDef, ValueSource source) {
            AkType shouldBe = fieldDef.column().getType().akType();
            if (shouldBe != source.getConversionType()) {
                ValueHolder holder = new ValueHolder();
                holder.expectType(shouldBe);
                Converters.convert(source, holder);
                source = holder;
            }
            return source;
        }

        @Override
        protected Object handleBigDecimal(FieldDef fieldDef, BigDecimal bigDecimal) {
            int size = fieldDef.getEncoding().widthFromObject(fieldDef, bigDecimal);
            byte[] bval = new byte[size];
            ConversionHelperBigDecimal.fromObject(fieldDef, bigDecimal, bval, 0);
            return bval;
        }

        @Override
        protected Object handleString(FieldDef fieldDef, String string) {
            String charset = fieldDef.column().getCharsetAndCollation().charset();
            try {
                return string.getBytes(charset);
            } catch (UnsupportedEncodingException e) {
                throw new AkibanInternalException("while decoding with charset " + charset, e);
            }
        }
    };
}
