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

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TParser;
import com.foundationdb.server.types.mcompat.MParsers;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.common.NumericFormatter;
import com.foundationdb.server.types.common.types.DoubleAttribute;
import com.foundationdb.server.types.common.types.SimpleDtdTClass;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.sql.types.TypeId;

public class MApproximateNumber extends SimpleDtdTClass
{
    public static final TClass DOUBLE = new MApproximateNumber("double", TypeId.DOUBLE_ID, 8, UnderlyingType.DOUBLE, MParsers.DOUBLE, NumericFormatter.FORMAT.DOUBLE, 22);
    public static final TClass DOUBLE_UNSIGNED = new MApproximateNumber("double unsigned", TypeId.DOUBLE_UNSIGNED_ID, 8, UnderlyingType.DOUBLE, MParsers.DOUBLE, NumericFormatter.FORMAT.DOUBLE, 22)
    {
        public TClass widestComparable()
        {
            return MNumeric.DECIMAL;
        }
    };

    public static final TClass FLOAT = new MApproximateNumber("float", TypeId.REAL_ID, 4, UnderlyingType.FLOAT, MParsers.FLOAT,  NumericFormatter.FORMAT.FLOAT, 12);
    public static final TClass FLOAT_UNSIGNED = new MApproximateNumber("float unsigned", TypeId.REAL_UNSIGNED_ID, 4, UnderlyingType.FLOAT, MParsers.FLOAT, NumericFormatter.FORMAT.FLOAT, 12);
    
    public static final int DEFAULT_DOUBLE_PRECISION = -1;
    public static final int DEFAULT_DOUBLE_SCALE = -1;

    private MApproximateNumber(String name, TypeId typeId, int serializationSize, UnderlyingType underlying, TParser parser,
                               TClassFormatter formatter, int defaultVarcharLen)
    {
        super(MBundle.INSTANCE.id(), name, AkCategory.FLOATING, formatter,
                DoubleAttribute.class,
                1, 1, serializationSize,
                underlying, parser, defaultVarcharLen, typeId);
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        return true;
    }

    @Override
    protected boolean attributeAlwaysDisplayed(int attributeIndex) {
        return false;
    }

    @Override
    public TInstance instance(boolean nullable)
    {
        return instance(DEFAULT_DOUBLE_PRECISION, DEFAULT_DOUBLE_SCALE, nullable);
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        int precisionL = left.attribute(DoubleAttribute.PRECISION);
        if (precisionL <= 0)
            return instance(suggestedNullability);
        int precisionR = right.attribute(DoubleAttribute.PRECISION);
        if (precisionR <= 0)
            return instance(suggestedNullability);

        int scaleL = left.attribute(DoubleAttribute.SCALE);
        int scaleR = right.attribute(DoubleAttribute.SCALE);

        return TBigDecimal.pickPrecisionAndScale(MApproximateNumber.this, precisionL, scaleL, precisionR, scaleR,
                suggestedNullability);
    }

    @Override
    protected void validate(TInstance type) {
        // TODO
    }
    
    public TClass widestComparable()
    {
        return DOUBLE;
    }
}
