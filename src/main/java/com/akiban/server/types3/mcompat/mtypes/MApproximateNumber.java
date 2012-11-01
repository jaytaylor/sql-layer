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
    public static final TClass DOUBLE_UNSIGNED = new MApproximateNumber("double unsigned", TypeId.DOUBLE_UNSIGNED_ID, PUnderlying.DOUBLE, TParsers.DOUBLE, NumericFormatter.FORMAT.DOUBLE, 22);
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
}
