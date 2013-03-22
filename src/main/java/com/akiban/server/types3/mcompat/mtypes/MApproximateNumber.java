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
package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TClassFormatter;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TParser;
import com.akiban.server.types3.TParsers;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.common.types.DoubleAttribute;
import com.akiban.server.types3.common.NumericFormatter;
import com.akiban.server.types3.common.types.SimpleDtdTClass;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.sql.types.TypeId;

public class MApproximateNumber extends SimpleDtdTClass
{
    public static final TClass DOUBLE = new MApproximateNumber("double", TypeId.DOUBLE_ID, PUnderlying.DOUBLE, TParsers.DOUBLE, NumericFormatter.FORMAT.DOUBLE, 22);
    public static final TClass DOUBLE_UNSIGNED = new MApproximateNumber("double unsigned", TypeId.DOUBLE_UNSIGNED_ID, PUnderlying.DOUBLE, TParsers.DOUBLE, NumericFormatter.FORMAT.DOUBLE, 22)
    {
        public TClass widestComparable()
        {
            return MNumeric.DECIMAL;
        }
    };

    public static final TClass FLOAT = new MApproximateNumber("float", TypeId.REAL_ID, PUnderlying.FLOAT, TParsers.FLOAT,  NumericFormatter.FORMAT.FLOAT, 12);
    public static final TClass FLOAT_UNSIGNED = new MApproximateNumber("float unsigned", TypeId.REAL_UNSIGNED_ID, PUnderlying.FLOAT, TParsers.FLOAT, NumericFormatter.FORMAT.FLOAT, 12);
    
    public static final int DEFAULT_DOUBLE_PRECISION = -1;
    public static final int DEFAULT_DOUBLE_SCALE = -1;

    private MApproximateNumber(String name, TypeId typeId, PUnderlying underlying, TParser parser,
                               TClassFormatter formatter, int defaultVarcharLen)
    {
        super(MBundle.INSTANCE.id(), name, AkCategory.FLOATING, formatter,
                DoubleAttribute.class,
                1, 1, 8,
                underlying, parser, defaultVarcharLen, typeId);
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        return true;
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

        return MBigDecimal.pickPrecisionAndScale(MApproximateNumber.this, precisionL, scaleL, precisionR, scaleR,
                suggestedNullability);
    }

    @Override
    protected void validate(TInstance instance) {
        // TODO
    }
    
    public TClass widestComparable()
    {
        return DOUBLE;
    }
}
