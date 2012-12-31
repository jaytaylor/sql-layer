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
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public final class MIfElse extends TScalarBase {

    public static final TScalar[] overloads = new TScalar[] {
            // If both inputs are of the same type (that's what an ANY-exact means), always use that
            new MIfElse(null, ExactInput.BOTH, -20),

            // Else, if either side is a string, use that. Note that each one of these are in their own cast group,
            // because we want the deciding type (e.g. LONGTEXT on the left side, in the first case below) to
            // unquestionably force the other input to be of its same type. If we put, for instance, all of the
            // left-is-stringy overloads into one group, then strongs-based considerations come into play, and since
            // virtually nothing is strongly castable to a varchar, none of these would get picked.
            new MIfElse(MString.LONGTEXT, ExactInput.LEFT, -10),
            new MIfElse(MString.LONGTEXT, ExactInput.RIGHT, -9),
            new MIfElse(MString.VARCHAR, ExactInput.LEFT, -8),
            new MIfElse(MString.VARCHAR, ExactInput.RIGHT, -7),

            // Ditto for the double types.
            new MIfElse(MApproximateNumber.DOUBLE, ExactInput.LEFT, -6),
            new MIfElse(MApproximateNumber.DOUBLE, ExactInput.RIGHT, -5),

            // Next, exact-precision types. Note that now we do put them into groups, since we actually want the
            // usual casting rules
            new MIfElse(MNumeric.DECIMAL, ExactInput.LEFT, -4),
            new MIfElse(MNumeric.BIGINT, ExactInput.LEFT, -4),
            new MIfElse(MNumeric.MEDIUMINT, ExactInput.LEFT, -4),
            new MIfElse(MNumeric.INT, ExactInput.LEFT, -4),
            new MIfElse(MNumeric.SMALLINT, ExactInput.LEFT, -4),
            new MIfElse(MNumeric.TINYINT, ExactInput.LEFT, -4),

            new MIfElse(MNumeric.DECIMAL_UNSIGNED, ExactInput.LEFT, -4),
            new MIfElse(MNumeric.BIGINT_UNSIGNED, ExactInput.LEFT, -4),
            new MIfElse(MNumeric.MEDIUMINT_UNSIGNED, ExactInput.LEFT, -4),
            new MIfElse(MNumeric.INT_UNSIGNED, ExactInput.LEFT, -4),
            new MIfElse(MNumeric.SMALLINT_UNSIGNED, ExactInput.LEFT, -4),
            new MIfElse(MNumeric.TINYINT_UNSIGNED, ExactInput.LEFT, -4),
            
            new MIfElse(MNumeric.DECIMAL, ExactInput.RIGHT, -4),
            new MIfElse(MNumeric.BIGINT, ExactInput.RIGHT, -4),
            new MIfElse(MNumeric.MEDIUMINT, ExactInput.RIGHT, -4),
            new MIfElse(MNumeric.INT, ExactInput.RIGHT, -4),
            new MIfElse(MNumeric.SMALLINT, ExactInput.RIGHT, -4),
            new MIfElse(MNumeric.TINYINT, ExactInput.RIGHT, -4),
            
            new MIfElse(MNumeric.DECIMAL_UNSIGNED, ExactInput.RIGHT, -4),
            new MIfElse(MNumeric.BIGINT_UNSIGNED, ExactInput.RIGHT, -4),
            new MIfElse(MNumeric.MEDIUMINT_UNSIGNED, ExactInput.RIGHT, -4),
            new MIfElse(MNumeric.INT_UNSIGNED, ExactInput.RIGHT, -4),
            new MIfElse(MNumeric.SMALLINT_UNSIGNED, ExactInput.RIGHT, -4),
            new MIfElse(MNumeric.TINYINT_UNSIGNED, ExactInput.RIGHT, -4),

            // Lastly, if nothing else works, treat both as varchar
            new MIfElse(MString.VARCHAR, ExactInput.NEITHER, -2),
    };

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(AkBool.INSTANCE, 0).pickingCovers(targetClass, 1, 2);
        if(exactInput == ExactInput.BOTH || exactInput == ExactInput.LEFT)
            builder.setExact(1, true);
        if(exactInput == ExactInput.BOTH || exactInput == ExactInput.RIGHT)
            builder.setExact(2, true);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        int whichSource = inputs.get(0).getBoolean() ? 1 : 2;
        PValueSource source = inputs.get(whichSource);
        PValueTargets.copyFrom(source, output);
    }

    @Override
    public String displayName() {
        return "IF";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return (inputIndex == 1);
    }

    @Override
    public int[] getPriorities() {
        return new int[] { priority };
    }

    @Override
    protected Constantness constness(int inputIndex, LazyList<? extends TPreptimeValue> values) {
        assert inputIndex == 0 : inputIndex; // should be fully resolved after the first call
        PValueSource condition = values.get(0).value();
        if (condition == null)
            return Constantness.NOT_CONST;
        int result = condition.getBoolean() ? 1 : 2;
        return values.get(result).value() == null ? Constantness.NOT_CONST : Constantness.CONST;
    }

    private MIfElse(TClass targetClass, ExactInput exactInput, int priority) {
        this.targetClass = targetClass;
        this.exactInput = exactInput;
        this.priority = priority;
    }

    private final TClass targetClass;
    private final ExactInput exactInput;
    private final int priority;

    private enum ExactInput {
        LEFT, RIGHT, NEITHER, BOTH
    }
}
