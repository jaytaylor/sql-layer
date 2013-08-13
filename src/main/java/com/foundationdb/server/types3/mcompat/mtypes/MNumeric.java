/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.types3.mcompat.mtypes;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types3.*;
import com.foundationdb.server.types3.aksql.AkCategory;
import com.foundationdb.server.types3.aksql.aktypes.AkBool;
import com.foundationdb.server.types3.common.types.NumericAttribute;
import com.foundationdb.server.types3.common.NumericFormatter;
import com.foundationdb.server.types3.common.types.SimpleDtdTClass;
import com.foundationdb.server.types3.mcompat.MBundle;
import com.foundationdb.server.types3.pvalue.PUnderlying;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.sql.types.TypeId;
import com.google.common.primitives.UnsignedLongs;

import java.lang.reflect.Field;
import java.math.BigInteger;

public class MNumeric extends SimpleDtdTClass {

    protected MNumeric(String name, TClassFormatter formatter, int serializationSize, PUnderlying pUnderlying,
                       int defaultWidth, TParser parser)
    {
        super(MBundle.INSTANCE.id(), name, AkCategory.INTEGER,
                formatter,
                NumericAttribute.class,
                1, 1, serializationSize, 
                pUnderlying, parser, defaultWidth, inferTypeid(name));
        this.defaultWidth = defaultWidth;
        this.isUnsigned = name.endsWith(" unsigned");
    }

    private static TypeId inferTypeid(String name) {
        // special cases first
        if ("mediumint".equals(name))
            return TypeId.INTEGER_ID;
        if ("mediumint unsigned".equals(name))
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
    public TInstance instance(boolean nullable) {
        return instance(defaultWidth, nullable);
    }

    @Override
    protected void validate(TInstance instance) {
        int m = instance.attribute(NumericAttribute.WIDTH);
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
    protected boolean tryFromObject(TExecutionContext context, PValueSource in, PValueTarget out) {
        if (in.tInstance().typeClass() == AkBool.INSTANCE) {
            byte asInt = (byte)(in.getBoolean() ? 1 : 0);
            switch (out.tInstance().typeClass().underlyingType()) {
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
    
    public boolean isUnsigned() {
        return isUnsigned;
    }
    
    private final int defaultWidth;
    private final boolean isUnsigned;
    
    // numeric types
    // TODO verify default widths
    public static final MNumeric TINYINT
            = new MNumeric("tinyint", NumericFormatter.FORMAT.INT_8, 1, PUnderlying.INT_8, 5, TParsers.TINYINT);

    public static final MNumeric TINYINT_UNSIGNED
            = new MNumeric("tinyint unsigned", NumericFormatter.FORMAT.INT_16, 4, PUnderlying.INT_16, 4, TParsers.UNSIGNED_TINYINT);

    public static final MNumeric SMALLINT
            = new MNumeric("smallint", NumericFormatter.FORMAT.INT_16, 2, PUnderlying.INT_16, 7, TParsers.SMALLINT);

    public static final MNumeric SMALLINT_UNSIGNED
            = new MNumeric("smallint unsigned", NumericFormatter.FORMAT.INT_32, 4, PUnderlying.INT_32, 6, TParsers.UNSIGNED_SMALLINT);

    public static final MNumeric MEDIUMINT
            = new MNumeric("mediumint", NumericFormatter.FORMAT.INT_32, 3, PUnderlying.INT_32, 9, TParsers.MEDIUMINT);

    public static final MNumeric MEDIUMINT_UNSIGNED
            = new MNumeric("mediumint unsigned", NumericFormatter.FORMAT.INT_64, 8, PUnderlying.INT_64, 8, TParsers.UNSIGNED_MEDIUMINT);

    public static final MNumeric INT
            = new MNumeric("integer", NumericFormatter.FORMAT.INT_32, 4, PUnderlying.INT_32, 11, TParsers.INT);

    public static final MNumeric INT_UNSIGNED
            = new MNumeric("integer unsigned", NumericFormatter.FORMAT.INT_64, 8, PUnderlying.INT_64, 10, TParsers.UNSIGNED_INT);

    public static final MNumeric BIGINT
            = new MNumeric("bigint", NumericFormatter.FORMAT.INT_64, 8, PUnderlying.INT_64, 21, TParsers.BIGINT)
            {
                public TClass widestComparable()
                {
                    return DECIMAL;
                }
            };

    public static final MNumeric BIGINT_UNSIGNED
            = new MNumeric("bigint unsigned", NumericFormatter.FORMAT.UINT_64, 8, PUnderlying.INT_64, 20, TParsers.UNSIGNED_BIGINT)
    {
        public TClass widestComparable()
        {
            return DECIMAL;
        }
        
        @Override
        protected PValueIO getPValueIO() {
            return bigintUnsignedIO;
        }
    };

    public static final TClass DECIMAL_UNSIGNED = new MBigDecimal("decimal unsigned", 10)
    {
        public TClass widestComparable()
        {
            return DECIMAL;
        }
    };
    
    public static long getAsLong(TClass tClass, PValueSource source) {
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

    public static void putAsLong(TClass tClass, PValueTarget target, long value) {
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

    public static final TClass DECIMAL = new MBigDecimal("decimal", 11);

    private static final PValueIO bigintUnsignedIO = new PValueIO() {
        @Override
        public void copyCanonical(PValueSource in, TInstance typeInstance, PValueTarget out) {
            out.putInt64(in.getInt64());
        }

        @Override
        public void writeCollating(PValueSource in, TInstance typeInstance, PValueTarget out) {
            String asString = UnsignedLongs.toString(in.getInt64());
            BigInteger asBigint = new BigInteger(asString);
            out.putObject(asBigint);
        }

        @Override
        public void readCollating(PValueSource in, TInstance typeInstance, PValueTarget out) {
            BigInteger asBigint = (BigInteger) in.getObject();
            long asLong = UnsignedLongs.parseUnsignedLong(asBigint.toString());
            out.putInt64(asLong);
        }
    };
}
