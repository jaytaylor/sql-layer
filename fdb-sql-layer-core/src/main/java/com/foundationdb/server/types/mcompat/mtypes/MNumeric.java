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

package com.foundationdb.server.types.mcompat.mtypes;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types.*;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.common.NumericFormatter;
import com.foundationdb.server.types.common.types.NumericAttribute;
import com.foundationdb.server.types.common.types.SimpleDtdTClass;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.server.types.mcompat.MParsers;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.sql.types.TypeId;
import com.google.common.primitives.UnsignedLongs;

import java.lang.reflect.Field;
import java.math.BigInteger;

public class MNumeric extends SimpleDtdTClass {

    protected MNumeric(String name, TClassFormatter formatter, int serializationSize, UnderlyingType underlyingType,
                       int defaultWidth, TParser parser)
    {
        super(MBundle.INSTANCE.id(), name, AkCategory.INTEGER,
                formatter,
                NumericAttribute.class,
                1, 1, serializationSize,
                underlyingType, parser, defaultWidth, inferTypeid(name));
        this.defaultWidth = defaultWidth;
        this.isUnsigned = name.endsWith(" unsigned");
    }

    private static TypeId inferTypeid(String name) {
        // special cases first
        if ("int".equals(name))
            return TypeId.INTEGER_ID;
        if ("int unsigned".equals(name))
            return TypeId.INTEGER_UNSIGNED_ID;

        name = name.toUpperCase().replace(' ', '_') + "_ID";
        try {
            Field field = TypeId.class.getField(name);
            // assume public and static. If not, Field.get will fail, which is all we can do anyway.
            Object instance = field.get(null);
            return (TypeId) instance;
        } catch (Exception e) {
            throw new AkibanInternalException("while getting field " + name, e);
        }
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        return false;
    }

    @Override
    protected boolean attributeAlwaysDisplayed(int attributeIndex) {
        return false;
    }

    @Override
    public TInstance instance(boolean nullable) {
        return instance(defaultWidth, nullable);
    }

    @Override
    protected void validate(TInstance type) {
        int m = type.attribute(NumericAttribute.WIDTH);
        if (m < 0 || m > 255)
            throw new TypeDeclarationException("width must be 0 < M < 256");
    }
    
    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        int leftWidth = left.attribute(NumericAttribute.WIDTH);
        int rightWidth = right.attribute(NumericAttribute.WIDTH);
        return instance(Math.max(leftWidth, rightWidth), suggestedNullability);
    }

    public TClass widestComparable()
    {
        return BIGINT;
    }

    @Override
    protected boolean tryFromObject(TExecutionContext context, ValueSource in, ValueTarget out) {
        if (in.getType().typeClass() == AkBool.INSTANCE) {
            byte asInt = (byte)(in.getBoolean() ? 1 : 0);
            switch (out.getType().typeClass().underlyingType()) {
            case INT_8:
                out.putInt8(asInt);
                return true;
            case INT_16:
                out.putInt16(asInt);
                return true;
            case UINT_16:
                out.putUInt16((char)asInt);
                return true;
            case INT_32:
                out.putInt32(asInt);
                return true;
            case INT_64:
                out.putInt64(asInt);
                return true;
            default:
                // fall through and keep trying the standard ways
            }
        }
        return super.tryFromObject(context, in, out);
    }
    
    @Override
    public boolean isUnsigned() {
        return isUnsigned;
    }
    
    @Override
    public int jdbcType() {
        int result = super.jdbcType();
        if (result == java.sql.Types.OTHER) {
            if ((this == MEDIUMINT) || (this == MEDIUMINT_UNSIGNED))
                // TODO: Maybe it would be better to fix TypeId in the parser.
                result = java.sql.Types.INTEGER;
            else
                assert false : this;
        }
        return result;
    }

    @Override
    public int fixedSerializationSize(TInstance type) {
        if (isUnsigned) {
            // This is the size for just storing the bits.
            if (this == TINYINT_UNSIGNED)
                return 1;
            else if (this == SMALLINT_UNSIGNED)
                return 2;
            else if (this == MEDIUMINT_UNSIGNED)
                return 3;
            else if (this == INT_UNSIGNED)
                return 4;
        }
        return super.fixedSerializationSize(type);
    }

    private final int defaultWidth;
    private final boolean isUnsigned;
    
    // numeric types
    // TODO verify default widths
    public static final MNumeric TINYINT
            = new MNumeric("tinyint", NumericFormatter.FORMAT.INT_8, 1, UnderlyingType.INT_8, 5, MParsers.TINYINT);

    public static final MNumeric TINYINT_UNSIGNED
            = new MNumeric("tinyint unsigned", NumericFormatter.FORMAT.INT_16, 4, UnderlyingType.INT_16, 4, MParsers.UNSIGNED_TINYINT);

    public static final MNumeric SMALLINT
            = new MNumeric("smallint", NumericFormatter.FORMAT.INT_16, 2, UnderlyingType.INT_16, 7, MParsers.SMALLINT);

    public static final MNumeric SMALLINT_UNSIGNED
            = new MNumeric("smallint unsigned", NumericFormatter.FORMAT.INT_32, 4, UnderlyingType.INT_32, 6, MParsers.UNSIGNED_SMALLINT);

    public static final MNumeric MEDIUMINT
            = new MNumeric("mediumint", NumericFormatter.FORMAT.INT_32, 3, UnderlyingType.INT_32, 9, MParsers.MEDIUMINT);

    public static final MNumeric MEDIUMINT_UNSIGNED
            = new MNumeric("mediumint unsigned", NumericFormatter.FORMAT.INT_64, 8, UnderlyingType.INT_64, 8, MParsers.UNSIGNED_MEDIUMINT);

    public static final MNumeric INT
            = new MNumeric("int", NumericFormatter.FORMAT.INT_32, 4, UnderlyingType.INT_32, 11, MParsers.INT);

    public static final MNumeric INT_UNSIGNED
            = new MNumeric("int unsigned", NumericFormatter.FORMAT.INT_64, 8, UnderlyingType.INT_64, 10, MParsers.UNSIGNED_INT);

    public static final MNumeric BIGINT
            = new MNumeric("bigint", NumericFormatter.FORMAT.INT_64, 8, UnderlyingType.INT_64, 21, MParsers.BIGINT)
            {
                public TClass widestComparable()
                {
                    return DECIMAL;
                }
            };

    public static final MNumeric BIGINT_UNSIGNED
            = new MNumeric("bigint unsigned", NumericFormatter.FORMAT.UINT_64, 8, UnderlyingType.INT_64, 20, MParsers.UNSIGNED_BIGINT)
    {
        public TClass widestComparable()
        {
            return DECIMAL;
        }
        
        @Override
        protected ValueIO getValueIO() {
            return bigintUnsignedIO;
        }

        @Override
        protected int doCompare(TInstance typeA, ValueSource sourceA, TInstance typeB, ValueSource sourceB) {
            return UnsignedLongs.compare(sourceA.getInt64(), sourceB.getInt64());
        }
    };

    public static final TClass DECIMAL_UNSIGNED = new MBigDecimal("decimal unsigned", 10)
    {
        public TClass widestComparable()
        {
            return DECIMAL;
        }
    };
    
    public static long getAsLong(TClass tClass, ValueSource source) {
        assert tClass instanceof MNumeric : "not an MNumeric: " + tClass;
        long result;
        switch (tClass.underlyingType()) {
        case INT_8:
            result = source.getInt8();
            break;
        case INT_16:
            result = source.getInt16();
            break;
        case UINT_16:
            result = source.getUInt16();
            break;
        case INT_32:
            result = source.getInt32();
            break;
        case INT_64:
            result = source.getInt64();
            break;
        default:
            throw new AssertionError(tClass.underlyingType() + ": " + tClass);
        }
        if ( ((MNumeric)tClass).isUnsigned && result < 0) {
            throw new IllegalStateException("can't get unsigned integer as long because it is too big: "
                    + UnsignedLongs.toString(result));
        }
        return result;
    }

    public static void putAsLong(TClass tClass, ValueTarget target, long value) {
        assert tClass instanceof MNumeric : "not an MNumeric: " + tClass;
        // TODO better bounds checking? Or do we just trust the caller?
        if ( ((MNumeric)tClass).isUnsigned && value < 0) {
            throw new IllegalStateException("can't get unsigned integer as long because it is too big: "
                    + UnsignedLongs.toString(value));
        }
        switch (tClass.underlyingType()) {
        case INT_8:
            target.putInt8((byte)value);
            break;
        case INT_16:
            target.putInt16((short)value);
            break;
        case UINT_16:
            target.putUInt16((char)value);
            break;
        case INT_32:
            target.putInt32((int)value);
            break;
        case INT_64:
            target.putInt64(value);
            break;
        default:
            throw new AssertionError(tClass.underlyingType() + ": " + tClass);
        }
    }

    public static final TBigDecimal DECIMAL = new MBigDecimal("decimal", 11);

    private static final ValueIO bigintUnsignedIO = new ValueIO() {
        @Override
        public void copyCanonical(ValueSource in, TInstance typeInstance, ValueTarget out) {
            out.putInt64(in.getInt64());
        }

        @Override
        public void writeCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
            String asString = UnsignedLongs.toString(in.getInt64());
            BigInteger asBigint = new BigInteger(asString);
            out.putObject(asBigint);
        }

        @Override
        public void readCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
            BigInteger asBigint = (BigInteger) in.getObject();
            long asLong = UnsignedLongs.parseUnsignedLong(asBigint.toString());
            out.putInt64(asLong);
        }
    };
}
