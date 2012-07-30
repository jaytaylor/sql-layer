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

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TCastPath;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TClassBase;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;

public final class Cast_From_Double {

    public static final TCast FLOAT_TO_DOUBLE = new TCastBase(MApproximateNumber.FLOAT, MApproximateNumber.DOUBLE) {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putDouble(source.getFloat());
        }
    };

    public static final TCast FLOAT_UNSIGNED_TO_DOUBLE = new TCastBase(MApproximateNumber.FLOAT_UNSIGNED, MApproximateNumber.DOUBLE) {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putDouble(source.getFloat());
        }
    };

    public static final TCast DOUBLE_UNSIGNED_TO_DOUBLE = new TCastBase(MApproximateNumber.DOUBLE_UNSIGNED, MApproximateNumber.DOUBLE) {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putDouble(source.getDouble());
        }
    };

    public static final TCastPath FLOAT_TO_VARCHAR = TCastPath.create(
            MApproximateNumber.FLOAT,
            MApproximateNumber.DOUBLE,
            MString.VARCHAR
    );

    public static final TCastPath FLOAT_UNSIGNED_TO_VARCHAR = TCastPath.create(
            MApproximateNumber.FLOAT_UNSIGNED,
            MApproximateNumber.DOUBLE,
            MString.VARCHAR
    );

    public static final TCastPath DOUBLE_UNSIGNED_TO_VARCHAR = TCastPath.create(
            MApproximateNumber.DOUBLE_UNSIGNED,
            MApproximateNumber.DOUBLE,
            MString.VARCHAR
    );
    public static final TCast TO_FLOAT = new TCastBase(MApproximateNumber.DOUBLE, MApproximateNumber.FLOAT) {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            double orig = source.getDouble();
            double positive = Math.abs(orig);
            if (positive > Float.MAX_VALUE) {
                context.reportTruncate(Double.toString(orig), Float.toString(Float.MAX_VALUE));
                orig = Float.MAX_VALUE; // TODO or is it null?
            }
            target.putFloat((float)orig);
        }
    };

    public static final TCast TO_FLOAT_UNSIGNED = new TCastBase(MApproximateNumber.DOUBLE, MApproximateNumber.FLOAT_UNSIGNED) {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            double orig = source.getDouble();
            if (orig < 0) {
                context.reportTruncate(Double.toString(orig), "0");
                orig = 0; // TODO or is it null?
            }
            else if (orig > Float.MAX_VALUE) {
                context.reportTruncate(Double.toString(orig), Float.toString(Float.MAX_VALUE));
                orig = Float.MAX_VALUE; // TODO or is it null?
            }
            target.putFloat((float)orig);
        }
    };

    public static final TCast TO_DOUBLE_UNSIGNED = new TCastBase(MApproximateNumber.DOUBLE, MApproximateNumber.DOUBLE_UNSIGNED, true) {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            double orig = source.getDouble();
            if (orig < 0) {
                context.reportTruncate(Double.toString(orig), "0");
                orig = 0; // TODO or is it null?
            }
            target.putDouble(orig);
        }
    };

    public static final TCast TO_VARCHAR = new TCastBase(MApproximateNumber.DOUBLE, MString.VARCHAR) {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            String asString = Double.toString(source.getDouble());
            int maxLen = context.outputTInstance().attribute(StringAttribute.LENGTH);
            if (asString.length() > maxLen) {
                String truncated = asString.substring(0, maxLen);
                context.reportTruncate(asString, truncated);
                asString = truncated;
            }
            target.putString(asString, null);
        }
    };
}
