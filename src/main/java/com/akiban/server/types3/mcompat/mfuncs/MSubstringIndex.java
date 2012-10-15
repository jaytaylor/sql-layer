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

import com.akiban.server.expression.std.Matchers.Index;
import com.akiban.server.types3.*;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import java.util.List;

public class MSubstringIndex extends TScalarBase {

    public static final TScalar INSTANCE = new MSubstringIndex();
    
    private static final int MATCHER_INDEX = 0;

    private MSubstringIndex(){}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MString.VARCHAR, 0, 1);
        builder.covers(MNumeric.INT, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        String str = inputs.get(0).getString();
        String substr = inputs.get(1).getString();
        int count = inputs.get(2).getInt32();
        boolean signed;

        if (count == 0 || str.isEmpty() || substr.isEmpty()) {
            output.putString("", null);
            return;
        } else if (signed = count < 0) {
            count = -count;
            str = new StringBuilder(str).reverse().toString();
            substr = new StringBuilder(substr).reverse().toString();
        }

        // try to reuse compiled pattern if possible
        Index matcher = (Index) context.exectimeObjectAt(MATCHER_INDEX);
        if (matcher == null || !matcher.sameState(substr, '\\')) {
            context.putExectimeObject(MATCHER_INDEX, matcher = new Index(substr));
        }

        int index = matcher.match(str, count);
        String ret = index < 0 // no match found
                ? str
                : str.substring(0, index);
        if (signed) {
            ret = new StringBuilder(ret).reverse().toString();
        }

        output.putString(ret, null);
    }

    @Override
    public String displayName() {
        return "SUBSTRING_INDEX";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TInstance stringInstance = inputs.get(0).instance();
                return MString.VARCHAR.instance(
                        stringInstance.attribute(StringAttribute.LENGTH),
                        anyContaminatingNulls(inputs));
            }
        });
    }
}
