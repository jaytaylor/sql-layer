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

package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import java.util.Arrays;
import java.util.Collection;

public final class MField extends TScalarBase {

    public static final Collection<? extends TScalar> exactOverloads = Collections2.transform(Arrays.asList(
            MNumeric.TINYINT,
            MNumeric.TINYINT_UNSIGNED,
            MNumeric.SMALLINT,
            MNumeric.SMALLINT_UNSIGNED,
            MNumeric.INT,
            MNumeric.INT_UNSIGNED,
            MNumeric.MEDIUMINT,
            MNumeric.MEDIUMINT_UNSIGNED,
            MNumeric.BIGINT,
            MNumeric.BIGINT_UNSIGNED,

            MNumeric.DECIMAL,
            MNumeric.DECIMAL_UNSIGNED,

            MString.VARCHAR,
            MString.TINYTEXT,
            MString.TEXT,
            MString.MEDIUMTEXT,
            MString.LONGTEXT
            ), new Function<TClass, TScalar>() {
                @Override
                public TScalar apply(TClass input) {
                    return new MField(input, true, 0);
                }
            }
    );
    public static final TScalar doubleOverload = new MField(MApproximateNumber.DOUBLE, false, 1);

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.setExact(exact).vararg(targetClass, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        PValueSource needle = inputs.get(0);
        int result = 0;
        if (!needle.isNull()) {
            TInstance needleInstance = context.inputTInstanceAt(0);
            for (int i = 1, size = inputs.size(); i < size; ++i) {
                PValueSource arg = inputs.get(i);
                if (!arg.isNull()) {
                    TInstance argInstance = context.inputTInstanceAt(i);
                    int cmp = TClass.compare(needleInstance, needle, argInstance, arg);
                    if (cmp == 0) {
                        result = i;
                        break;
                    }
                }
            }
        }
        output.putInt32(result);
    }

    @Override
    public String displayName() {
        return "FIELD";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.INT.instance(3));
    }

    @Override
    public int[] getPriorities() {
        int[] result = new int[maxPriority+1];
        for (int i = 0; i < maxPriority; ++i)
            result[i] = i;
        return result;
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    private MField(TClass targetClass, boolean exact, int maxPriority) {
        this.targetClass = targetClass;
        this.exact = exact;
        this.maxPriority = maxPriority;
    }

    private final TClass targetClass;
    private final int maxPriority;
    private final boolean exact;
}
