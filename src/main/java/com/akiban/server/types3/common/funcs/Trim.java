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
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.*;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class Trim extends TScalarBase {

    // Described by {L,R,}TRIM(<string_to_trim>, <char_to_trim>)
    public static TScalar[] create(TClass stringType) {
        TScalar rtrim = new Trim(stringType) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                String st = inputs.get(0).getString();
                String trim = inputs.get(1).getString();
                
                if (!isValidInput(trim.length(), st.length())) {
                    output.putString(st, null);
                    return;
                }
                output.putString(rtrim(st, trim), null);
            }

            @Override
            public String displayName() {
                return "RTRIM";
            }
        };

        TScalar ltrim = new Trim(stringType) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                String st = inputs.get(0).getString();
                String trim = inputs.get(1).getString();
                
                if (!isValidInput(trim.length(), st.length())) {
                    output.putString(st, null);
                    return;
                }
                output.putString(ltrim(st, trim), null);
            }

            @Override
            public String displayName() {
                return "LTRIM";
            }
        };

        TScalar trim = new Trim(stringType) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                String st = inputs.get(0).getString();
                String trim = inputs.get(1).getString();
                
                if (!isValidInput(trim.length(), st.length())) {
                    output.putString(st, null);
                    return;
                }

                st = rtrim(ltrim(st, trim), trim);
                output.putString(st, null);
            }

            @Override
            public String displayName() {
                return "TRIM";
            }
        };
        
        return new TScalar[]{ltrim, rtrim, trim};
    }

    protected final TClass stringType;

    private Trim(TClass stringType) {
        this.stringType = stringType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(stringType, 0, 1);
    }

    @Override
    public TOverloadResult resultType() {
        // actual return type is exactly the same as input type
        return TOverloadResult.fixed(stringType.instance());
    }

    // Helper methods
    protected static String ltrim(String st, String trim) {
        int n, count;
        n = count = 0;
        while (n < st.length()) {
            count = 0;
            for (int i = 0; i < trim.length() && n < st.length(); ++i, ++n) {
                if (st.charAt(n) != trim.charAt(i)) return st.substring(n-i);
                else count++;
            }
        }
        return count == trim.length() ? "" : st.substring(n-count);
    }

    protected static String rtrim(String st, String trim) {
        int n = st.length() - 1;
        int count = 0;
        while (n >= 0) {
            count = 0;
            for (int i = trim.length()-1; i >= 0 && n >= 0; --i, --n) {
                if (st.charAt(n) != trim.charAt(i))
                    return st.substring(0, n + trim.length() - i);
                else count++;
            }
        }
        return count == trim.length() ? "" : st.substring(0, count);
    }
    
    protected static boolean isValidInput(int trimLength, int strLength) {
        return trimLength != 0 && strLength != 0 && trimLength <= strLength;
    }
}
