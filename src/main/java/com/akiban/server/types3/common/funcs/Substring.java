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
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;

public abstract class Substring extends TOverloadBase {

    public static TOverload[] create(TClass stringType, TClass numericType) {
        TOverload twoArgs =
                new Substring(stringType, numericType) {

                    @Override
                    protected void buildInputSets(TInputSetBuilder builder) {
                        builder.covers(stringType, 0);
                        builder.covers(numericType, 1);
                    }

                    @Override
                    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                        String str = inputs.get(0).getString();
                        int from = adjustIndex(str, inputs.get(1).getInt32());

                        if (from == -1) {
                            output.putString("", null);
                        } else {
                            output.putString(getSubstring(str.length() - 1, from, str), null);
                        }
                    }

                    @Override
                    public TOverloadResult resultType() {
                        return TOverloadResult.custom(new TCustomOverloadResult() {

                            @Override
                            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                                TPreptimeValue preptimeValue = inputs.get(0);
                                int stringLength = preptimeValue.instance().attribute(StringAttribute.LENGTH);
                                int stringCharsetId = preptimeValue.instance().attribute(StringAttribute.CHARSET);

                                final int offset, calculatedLength;
                                TPreptimeValue offsetValue = inputs.get(1);
                                if (offsetValue.value() == null) {
                                    offset = 0; // assume string starts at beginning
                                } else {
                                    offset = offsetValue.value().getInt32();
                                }

                                calculatedLength = calculateCharLength(stringLength, offset, Integer.MAX_VALUE);
                                return stringType.instance(calculatedLength, stringCharsetId);
                            }
                        });
                    }
                };
        TOverload threeArgs =
                new Substring(stringType, numericType) {

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(stringType, 0);
                builder.covers(numericType, 1, 2);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                String str = inputs.get(0).getString();
                int length = str.length();
                int from = adjustIndex(str, inputs.get(1).getInt32());

                if (from == -1) {
                    output.putString("", null);
                } else {
                    output.putString(getSubstring(from + inputs.get(2).getInt32() - 1, from, str), null);
                }
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
                        if (offsetValue.value() == null) {
                            offset = 0; // assume string starts at beginning
                        } else {
                            offset = offsetValue.value().getInt32();
                        }

                        TPreptimeValue substringLengthValue = inputs.get(2);
                        if (substringLengthValue.value() == null) {
                            substringLength = Integer.MAX_VALUE; // assume string is unbounded
                        } else {
                            substringLength = substringLengthValue.value().getInt32();
                        }

                        calculatedLength = calculateCharLength(stringLength, offset, substringLength);
                        return stringType.instance(calculatedLength, stringCharsetId);
                    }
                });
            }
        };
        return new TOverload[]{twoArgs, threeArgs};
    }
    protected final TClass stringType;
    protected final TClass numericType;

    private Substring(TClass stringType, TClass numericType) {
        this.stringType = stringType;
        this.numericType = numericType;
    }

    @Override
    public String displayName() {
        return "SUBSTRING";
    }

    @Override
    public String[] registeredNames()
    {
        return new String[]{"substring", "substr"};
    }

    protected int adjustIndex(String str, int index) {
        // String operand
        if (str.equals("")) {
            return -1;
        }

        // if from is negative or zero, start from the end, and adjust
        // index by 1 since index in sql starts at 1 NOT 0
        index += (index < 0 ? str.length() : -1);

        // if from is still neg, return -1
        if (index < 0) {
            return -1;
        }

        return index;
    }

    protected String getSubstring(int to, int from, String str) {
        // if to <= fr => return empty
        if (to < from || from >= str.length()) {
            return "";
        }

        to = (to > str.length() - 1 ? str.length() - 1 : to);
        return str.substring(from, to + 1);
    }

    protected int calculateCharLength(int stringLength, int offset, int substringLength) {
        if (stringLength < Math.abs(offset) || substringLength <= 0) {
            return 0;
        }
        int ret = offset > 0 ? stringLength - offset + 1 : offset * -1;
        return Math.min(ret, substringLength);
    }
}
