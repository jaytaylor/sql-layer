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
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A)
 * DO NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS
 * OF YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO
 * SIGN FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT.
 * THE LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON
 * ACCEPTANCE BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.*;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;

public abstract class Trim extends TOverloadBase {

    public static TOverload[] create(TClass stringType) {
        TOverload rtrim = new Trim(stringType, TrimType.TRAILING) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                String st = (String) inputs.get(0).getObject();
                output.putObject(ltrim(st, DEFAULT_TRIM));
            }
        };

        TOverload ltrim = new Trim(stringType, TrimType.LEADING) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                String st = (String) inputs.get(0).getObject();
                output.putObject(rtrim(st, DEFAULT_TRIM));
            }
        };

        TOverload trim = new Trim(stringType, null) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                String st = (String) inputs.get(0).getObject();
                st = ltrim(st, DEFAULT_TRIM);
                output.putObject(rtrim(st, DEFAULT_TRIM));
            }
        };
        
        // TODO: support LEADING, TRAILING, BOTH options in TRIM
        return new TOverload[]{ltrim, rtrim, trim};
    }

    public static enum TrimType {

        LEADING, TRAILING
    };
    protected final TClass stringType;
    protected final TrimType trimType;
    protected static final char DEFAULT_TRIM = ' ';

    private Trim(TClass stringType, TrimType trimType) {
        this.stringType = stringType;
        this.trimType = trimType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(stringType, 0);
    }

    @Override
    public String overloadName() {
        String name = "";
        if (trimType == TrimType.LEADING) {
            name += "L";
        }
        if (trimType == TrimType.TRAILING) {
            name += "R";
        }
        return name + "TRIM";
    }

    @Override
    public TOverloadResult resultType() {
        // actual return type is exactly the same as input type
        return TOverloadResult.custom(new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                return inputs.get(0).instance();
            }
        });
    }

    // Helper methods
    protected static String ltrim(String st, char ch) {
        for (int n = 0; n < st.length(); ++n) {
            if (st.charAt(n) != ch) {
                return st.substring(n);
            }
        }
        return "";
    }

    protected static String rtrim(String st, char ch) {
        for (int n = st.length() - 1; n >= 0; --n) {
            if (st.charAt(n) != ch) {
                return st.substring(0, n + 1);
            }
        }
        return "";
    }
}
