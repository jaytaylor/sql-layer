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
package com.foundationdb.qp.row;

import java.util.EnumMap;

import com.foundationdb.ais.model.HKeyColumn;
import com.foundationdb.ais.model.HKeySegment;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.rowtype.HKeyRowType;
import com.foundationdb.server.store.PersistitKeyAppender;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;
import com.persistit.Key;

public class ValuesHKey extends AbstractValuesHolderRow implements HKey {

    @Override
    public int compareTo(HKey o) {
        assert o instanceof ValuesHKey : "Wrong HKey Type for compareTo";
        ValuesHKey that = (ValuesHKey)o;
        int columnIndex = 0;
        int cmp;
        for (int segment = 0; segment < hKeySegments; segment++) {
            cmp = this.ordinals[segment] - that.ordinals[segment];
            if (cmp != 0) {
                return cmp;
            }
            for (int i = 0; i < rowType().hKey().segments().get(segment).columns().size(); i++ ) {
                if (!that.values.get(columnIndex).hasAnyValue()) {
                    return 1;
                } else if (!this.values.get(columnIndex).hasAnyValue()) {
                    return -1;
                } else {
                    if (! TClass.comparisonNeedsCasting(this.values.get(columnIndex).getType(), 
                            that.values.get(columnIndex).getType())) {
                        cmp = TClass.compare(this.values.get(columnIndex).getType(), 
                                this.values.get(columnIndex), 
                                that.values.get(columnIndex).getType(), 
                                that.values.get(columnIndex));
                        if (cmp != 0) {
                            return cmp;
                        }
                    } else {
                        // TODO: Is this Correct? If the two columns are not comparable, which goes first? 
                        // problem: the prefixOf uses this code, and that should return false for this case
                        throw new UnsupportedOperationException();
                    }
                }
            }
            columnIndex++;
        }
        return 0;
    }

    @Override
    public boolean prefixOf(HKey hKey) {
        assert hKey instanceof ValuesHKey : "Wrong HKey Type for prefixOf";
        ValuesHKey that = (ValuesHKey)hKey;

        if (this.rowType().table().getGroup() != that.rowType().table().getGroup()) 
            return false;
        if (this.hKeySegments <= that.hKeySegments) {
            int cmp = compareTo(that);
            if (cmp == 0) return true;
        }
        return false;
    }

    @Override
    public int segments() {
        return hKeySegments;
    }

    @Override
    public void useSegments(int segments) {
        assert segments > 0 && segments < ordinals.length : segments;
        hKeySegments = segments;
    }

    @Override
    public void copyTo(HKey target) {
        assert target instanceof ValuesHKey : "Wrong HKey Type for copyTo";
        ValuesHKey t = (ValuesHKey)target;

        int columnIndex = 0;
        
        for (int segment = 0; segment < hKeySegments; segment++) {
            if (segment >= t.hKeySegments) break;
            assert ordinals[segment] == t.ordinals[segment] : "Mismatched ordinals on segment " + segment +" for " + ordinals[segment] + " source vs " + t.ordinals[segment] + " target";
            for (int i = 0 ; i < rowType().hKey().segments().get(segment).columns().size(); i++) {
                Value valueSource = valueAt(columnIndex);
                if (valueSource.hasAnyValue()) {
                    Value valueTarget = t.valueAt(columnIndex);
                    ValueTargets.copyFrom(valueSource, valueTarget);
                } else {
                    //t.valueAt(columnIndex).putNull();
                }
                columnIndex++;
            }
        }
        if (t.hKeySegments > hKeySegments)
            t.useSegments(hKeySegments);
        
        //for (int i = columnIndex; i < t.values.size(); i++) {
        //    t.valueAt(i).putNull();
        //}
    }
    
    @Override 
    public Key key() {
        throw new UnsupportedOperationException("Key() not supported");
    }

    @Override
    public void copyTo (Key target) {
        int columnIndex = 0;
        
        PersistitKeyAppender appender = PersistitKeyAppender.create(target, rowType().table().getName() );
        for (int segment = 0; segment < hKeySegments; segment++) {
            appender.append(ordinals[segment]);
            for (HKeyColumn column : rowType().hKey().segments().get(segment).columns()) {
                if (this.values.get(columnIndex).hasAnyValue()) {
                    appender.append(this.values.get(columnIndex), column.column());
                } else {
                    //appender.appendNull();
                }
                columnIndex++;
            }
        }
    }

    @Override
    public HKey ancestorHKey(Table table)
    {
        // TODO: This does the wrong thing for hkeys derived from group index rows!
        // TODO: See bug 997746.
        HKeyRowType rowType = this.rowType().schema().newHKeyRowType(table.hKey());
        HKey ancestorHKey = new ValuesHKey(rowType);
        copyTo(ancestorHKey);
        ancestorHKey.useSegments(table.getDepth() + 1);
        return ancestorHKey;
    }


    @Override 
    public Key key(Key start) {
        this.copyTo(start);
        return start;
    }
    
    @Override
    @Deprecated
    public void extendWithOrdinal(int ordinal) {
        ordinals[hKeySegments++] = ordinal;
    }

    @Override
    @Deprecated
    public void extendWithNull() {
        throw new UnsupportedOperationException();
        /*
        assert extendIndex < rowType().hKey().nColumns() : "Too may columns in HKey";
        TInstance tInstance = rowType().hKey().column(extendIndex++).getType();
        Value value = new Value (tInstance);
        value.putNull();
        values.add(value);
        */
    }

    @Override
    public ValueSource pEval(int valueIndex) {
        return values.get(valueIndex);
    }

    public ValueTarget pTarget (int valueIndex) {
        return values.get(valueIndex);
    }

    @Override
    public void copyFrom(Key source)
    {
        int sourceDepth = source.getDepth();
        assert sourceDepth >= 1 : "No data in source key";
        source.indexTo(0);
        int segment = 0;
        int columnsInSegment = 0;
        
        int ordinal = source.decodeInt();
        sourceDepth--;
        assert ordinal == ordinals[segment] : "Wrong ordinal for segment from key";
        for (int i = 0; i < values.size(); i++) {
            if (columnsInSegment == keyDepth[segment] ) {
                segment++;
                ordinal = source.decodeInt();
                assert ordinals[segment] == ordinal : "Mismatched ordinals " + ordinals[segment] + " source vs " + ordinal + " target";
                columnsInSegment = 0;
                sourceDepth--;
            }
            if (sourceDepth <= 0|| source.isNull(true)) {
                values.get(i).putNull();
            } else {
                sourceDepth--;
                Value valueTarget = values.get(i);
                UnderlyingType underlyingType = TInstance.underlyingType(valueTarget.getType());
                Class<?> expected = underlyingExpectedClasses.get(underlyingType);
                if (source.decodeType() == expected) {
                    switch (underlyingType) {
                        case BOOL:      valueTarget.putBool(source.decodeBoolean());        break;
                        case INT_8:     valueTarget.putInt8((byte)source.decodeLong());     break;
                        case INT_16:    valueTarget.putInt16((short)source.decodeLong());   break;
                        case UINT_16:   valueTarget.putUInt16((char)source.decodeLong());   break;
                        case INT_32:    valueTarget.putInt32((int)source.decodeLong());     break;
                        case INT_64:    valueTarget.putInt64(source.decodeLong());          break;
                        case FLOAT:     valueTarget.putFloat(source.decodeFloat());         break;
                        case DOUBLE:    valueTarget.putDouble(source.decodeDouble());       break;
                        case BYTES:     valueTarget.putBytes(source.decodeByteArray());     break;
                        case STRING:    valueTarget.putString(source.decodeString(), null); break;
                        default: throw new UnsupportedOperationException(valueTarget.getType() + " with " + underlyingType);
                    }
                }
                else {
                    valueTarget.putObject(source.decode());
                }
                // the following assumes that the TClass' readCollating expects the same UnderlyingType for in and out
                valueTarget.getType().readCollating(valueTarget, valueTarget);
            }
            columnsInSegment++;
        }        
    }

    public ValuesHKey (HKeyRowType rowType)
    {
        super(rowType, true);
        this.hKeySegments = rowType().hKey().segments().size();
        setOrdinals();
    }

    private void setOrdinals() {
        this.ordinals = new int [hKeySegments+1];
        this.keyDepth = new int [hKeySegments];
        int ordinalIndex = 0;
        for (HKeySegment hKeySegment : rowType().hKey().segments()) {
            Table segmentTable = hKeySegment.table();
            ordinals[ordinalIndex] = segmentTable.getOrdinal();
            keyDepth[ordinalIndex] = hKeySegment.columns().size();
            ordinalIndex++;
        }
    }
    
    // For testing purposes
    protected int[] ordinals() { return ordinals; }
    
    private int[] ordinals;
    private int[] keyDepth;
    private int hKeySegments;
//    private int extendIndex = 0;

    private static final EnumMap<UnderlyingType, Class<?>> underlyingExpectedClasses = createPUnderlyingExpectedClasses();

    private static EnumMap<UnderlyingType, Class<?>> createPUnderlyingExpectedClasses() {
        EnumMap<UnderlyingType, Class<?>> result = new EnumMap<>(UnderlyingType.class);
        for (UnderlyingType underlyingType : UnderlyingType.values()) {
            final Class<?> expected;
            switch (underlyingType) {
            case BOOL:
                expected = Boolean.class;
                break;
            case INT_8:
            case INT_16:
            case UINT_16:
            case INT_32:
            case INT_64:
                expected = Long.class;
                break;
            case FLOAT:
                expected = Float.class;
                break;
            case DOUBLE:
                expected = Double.class;
                break;
            case BYTES:
                expected = byte[].class;
                break;
            case STRING:
                expected = String.class;
                break;
            default:
                throw new AssertionError("unrecognized UnderlyingType: " + underlyingType);
            }
            result.put(underlyingType, expected);
        }
        return result;
    }

}


