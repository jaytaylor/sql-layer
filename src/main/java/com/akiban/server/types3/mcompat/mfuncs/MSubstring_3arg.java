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

import com.akiban.server.types3.*;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import java.util.List;

public class MSubstring_3arg extends MSubstring {

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MString.VARCHAR, 0);
        builder.covers(MNumeric.INT, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        String str = (String) inputs.get(0).getObject();
        int length = str.length();
        int from = adjustIndex(str, inputs.get(1).getInt32());
        
        if (from == -1) output.putObject("");
        else output.putObject(getSubstring(from + inputs.get(2).getInt32() -1, from, str));
    }
    
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TPreptimeValue preptimeValue = inputs.get(0);
                int stringLength = preptimeValue.instance().attribute(StringAttribute.LENGTH);
                int stringCharsetId = preptimeValue.instance().attribute(StringAttribute.CHARSET);
                
                final int offset, substringLength, calculatedLength;
                TPreptimeValue offsetValue = inputs.get(1);
                if (offsetValue.value() == null)
                    offset = 0; // assume string starts at beginning
                else 
                    offset = offsetValue.value().getInt32();
                
                TPreptimeValue substringLengthValue = inputs.get(2);
                if (substringLengthValue.value() == null)
                    substringLength = Integer.MAX_VALUE; // assume string is unbounded
                else
                    substringLength = substringLengthValue.value().getInt32();
                
                calculatedLength = calculateCharLength(stringLength, offset, substringLength);
                return MString.VARCHAR.instance(calculatedLength, stringCharsetId);
            }
        });
    }  
}
