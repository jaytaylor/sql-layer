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

import java.util.Arrays;
import java.util.EnumMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.HKeyColumn;
import com.foundationdb.ais.model.HKeySegment;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.rowtype.HKeyRowType;
import com.foundationdb.server.store.PersistitKeyAppender;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTargets;
import com.persistit.Key;

public class ValuesHKey extends AbstractValuesHolderRow implements HKey {

    // HKey interface - Object interface
    @Override
    public boolean equals(Object that)
    {
        return
            that == this ||
            (that != null &&
                that instanceof ValuesHKey &&
                this.compareTo((ValuesHKey)that) == 0);
    }

    @Override
    // TODO: This is overkill, but several tests rely upon the older PersistitHKey(key#toString()) behavior, 
    // See ITBase#compareTwoRows(line 190ff). 
    public String toString() {
        Key target = new Key (null, 2047);
        this.copyTo(target);
        return target.toString();
    }
    
    
    // HKey Interface  - Comparable interface 
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
                    ValueSource left = this.values.get(columnIndex);
                    ValueSource right = that.values.get(columnIndex);
                    TKeyComparable compare = registry.getKeyComparable(this.values.get(columnIndex).getType().typeClass(), 
                            that.values.get(columnIndex).getType().typeClass());
                    if (compare != null) {
                        cmp =  compare.getComparison().compare(left.getType(), left, right.getType(), right);
                    } else {
                        cmp = TClass.compare(left.getType(), left, right.getType(), right);
                    }
                    if (cmp != 0) {
                        return cmp;
                    }
                }
            }
            columnIndex++;
        }
        return 0;
    }

    // HKey interface implementation
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
            for (int i = 0 ; i < keyDepth[segment]; i++) {
                copyValue (valueAt(columnIndex), t.valueAt(columnIndex));
                columnIndex++;
            }
        }
        if (t.hKeySegments > hKeySegments)
            t.useSegments(hKeySegments);
    }
    
    public void copyValueTo(ValueSource value, int columnIndex) {
        copyValue (value, valueAt(columnIndex));
    }
    
    @Override
    public void extendWithOrdinal(int ordinal) {
        
        // There are two cases where we want to extend this HKey
        // This (child hkey) was copied from parent, and now extended to match this child
        if (hKeySegments < ordinals.length-1 ) {

            //LOG.trace("Attempting to assign ordinal " + ordinal + " to position " + hKeySegments + " into " + Arrays.toString(ordinals));
            
            int finalSegment = 0;
            for (int segment = hKeySegments - 1; segment < ordinals.length-1; segment++) {
                if (ordinals[segment] == ordinal)
                    finalSegment = segment;
            }
            assert finalSegment != 0 : "Attempting to assign ordinal " + ordinal + " to position " + hKeySegments + " into " + Arrays.toString(ordinals);
            
            for (int segment = hKeySegments; segment < finalSegment; segment++) {
                hKeySegments++;
                extendWithNull();
            }
            hKeySegments++;
        } else {
            // This (parent hkey) is being extended to query a specific child
            ordinals[hKeySegments++] = ordinal;
        }
    }

    
    @Override
    public void extendWithNull() {
        int columnIndex = -1;
        int segment;
        for (segment = 0; segment < hKeySegments; segment++) {
            columnIndex += keyDepth[segment];
        }
        for (int i = 0; i < keyDepth[segment-1] && columnIndex < values.size() ; columnIndex++, i++) {
            valueAt(columnIndex).putNull();
        }
    }

    // HKey interface implementation - low level interface (to be removed at some point)
    
    @Override
    public void copyTo (Key target) {
        int columnIndex = 0;
        PersistitKeyAppender appender = PersistitKeyAppender.create(target, rowType().table().getName() );
        int maxSegments = Math.min(hKeySegments, rowType().hKey().segments().size());
        for (int segment = 0; segment < maxSegments; segment++) {
            appender.append(ordinals[segment]);
            for (HKeyColumn column : rowType().hKey().segments().get(segment).columns()) {
                if (this.values.get(columnIndex).hasAnyValue()) {
                    appender.append(this.values.get(columnIndex), column.column());
                }
                columnIndex++;
            }
        }
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
    

    
    // AbstractHoldersRow overrides 
    
    @Override
    public HKey hKey() {
        return this;
    }

    public Value valueAt(int index) {
        return super.valueAt(index);
    }

    @Override
    public HKey ancestorHKey(Table table)
    {
        // TODO: This does the wrong thing for hkeys derived from group index rows!
        // TODO: See bug 997746.
        HKeyRowType rowType = this.rowType().schema().newHKeyRowType(table.hKey());
        HKey ancestorHKey = new ValuesHKey(rowType, this.registry);
        copyTo(ancestorHKey);
        ancestorHKey.useSegments(table.getDepth() + 1);
        return ancestorHKey;
    }

    // Constructors and private methods. 
    
    public ValuesHKey (HKeyRowType rowType, TypesRegistryService registry)
    {
        super(rowType, true);
        this.registry = registry;
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

    private void copyValue (ValueSource valueSource, Value valueTarget) {
        if (valueSource.hasAnyValue()) {
            TKeyComparable compare = registry.getKeyComparable(valueSource.getType().typeClass(), valueTarget.getType().typeClass());
            if (compare != null) {
                compare.getComparison().copyComparables(valueSource, valueTarget);
            } else {
                ValueTargets.copyFrom(valueSource, valueTarget);
            }
        }        
    }
    // For testing purposes
    protected int[] ordinals() { return ordinals; }
    
    private int[] ordinals;
    private int[] keyDepth;
    private int hKeySegments;
    private static final Logger LOG = LoggerFactory.getLogger(ValuesHKey.class);
    private final TypesRegistryService registry;

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


