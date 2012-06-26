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

import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.types.NumericAttribute;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.common.types.StringFactory.Charset;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TCastBase;

public final class Cast_MInt_MVarchar extends TCastBase {
    @Override
    public TClass sourceClass() {
        return MNumeric.INT;
    }

    @Override
    public TClass targetClass() {
        return MString.VARCHAR;
    }

    @Override
    public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance ins) {
        PValueSource val = preptimeInput.value();
        int charLen;
        if (val != null) {
            String result = getString(val);
            context.set(CAHCE_INDEX, result);
            charLen = result.length();
        }
        else {
            charLen = preptimeInput.instance().attribute(NumericAttribute.WIDTH);
        }
        return MString.VARCHAR.instance(charLen, Charset.LATIN1.ordinal(), -1);
    }

    @Override
    public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
    {
        String result = context.hasExectimeObject(CAHCE_INDEX)
                ? (String) context.objectAt(CAHCE_INDEX)
                : getString(source);
        int maxLen = context.outputTInstance().attribute(StringAttribute.LENGTH);
        if (result.length() > maxLen) {
            assert false : "what's our truncation error?";
            context.warnClient(null); // TODO what's our truncation error?
            result = result.substring(0, maxLen);
        }
        target.putObject(result);
    }

    private String getString(PValueSource source) {
        return Integer.toString(source.getInt32());
    }

    private static final int CAHCE_INDEX = 0;
}
