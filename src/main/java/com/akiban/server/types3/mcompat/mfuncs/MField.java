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

public final class MField extends TScalarBase {

    public static final TScalar[] exactOverloads = new TScalar[] {
            // From the MySQL docs:
            //
            //     If all arguments to FIELD() are strings, all arguments are compared as strings.
            //     If all arguments are numbers, they are compared as numbers.
            //     Otherwise, the arguments are compared as double.
            //
            // We can accomplish this by having three ScalarsGroups. The first consist of VARCHAR and CHAR. We want
            // more than one TScalar in this group, so that same-type-at is false and it's not automatically picked.
            // Only the various string types are strongly castable to either of these, so that takes care of the first
            // sentence in the FIELD spec (as quoted above). The second ScalarsGroup consists of exact numbers. We
            // provide these in four flavors: signed and unsigned of BIGINT and DECIMAL. This lets us cover all of the
            // exact precision types without expensive casts (casts between non-DECIMAL ints are cheap). Finally,
            // the DOUBLE overload is in a ScalarsGroup by itself, so that same-type-at is true and the scalar
            // is always picked.

            new MField(MString.VARCHAR, false, 1),
            new MField(MString.CHAR, false, 1),

            new MField(MNumeric.BIGINT, false, 2),
            new MField(MNumeric.BIGINT_UNSIGNED, false, 2),
            new MField(MNumeric.DECIMAL, false, 2),
            new MField(MNumeric.DECIMAL_UNSIGNED, false, 2),

            new MField(MApproximateNumber.DOUBLE, false, 3)
    };

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
        return TOverloadResult.fixed(MNumeric.INT, 3);
    }

    @Override
    public int[] getPriorities() {
        return new int[] { priority };
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    private MField(TClass targetClass, boolean exact, int maxPriority) {
        this.targetClass = targetClass;
        this.exact = exact;
        this.priority = maxPriority;
    }

    private final TClass targetClass;
    private final int priority;
    private final boolean exact;
}
