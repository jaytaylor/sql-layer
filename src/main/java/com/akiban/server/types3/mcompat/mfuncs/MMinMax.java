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
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class MMinMax extends TScalarBase {
    public static final TScalar INSTANCES[] = new TScalar[]
    {
        new MMinMax ("_min", MNumeric.SMALLINT) {
            @Override
            protected int getIndex(PValueSource first,PValueSource second) {
                return first.getInt16() < second.getInt16() ? 0 : 1;
            }
        },
        new MMinMax ("_min", MNumeric.INT) {
            @Override
            protected int getIndex(PValueSource first,PValueSource second) {
                return first.getInt32() < second.getInt32() ? 0 : 1;
            }
        },
        new MMinMax ("_min", MNumeric.BIGINT) {
            @Override
            protected int getIndex(PValueSource first,PValueSource second) {
                return first.getInt64() < second.getInt64() ? 0 : 1;
            }
        },
        new MMinMax ("_min", MApproximateNumber.DOUBLE) {
            @Override
            protected int getIndex(PValueSource first,PValueSource second) {
                return first.getDouble() < second.getDouble() ? 0 : 1;
            }
        },
        new MMinMax ("_min", MString.VARCHAR) {
            @Override
            protected int getIndex (PValueSource first, PValueSource second) {
                return first.getString().compareTo(second.getString()) < 0 ? 0 : 1;
            }
        },
        new MMinMax ("_max", MNumeric.SMALLINT) {
            @Override
            protected int getIndex(PValueSource first,PValueSource second) {
                return first.getInt16() > second.getInt16() ? 0 : 1;
            }
        },
        new MMinMax ("_max", MNumeric.INT){
            @Override
            protected int getIndex (PValueSource first, PValueSource second) {
                return first.getInt32() > second.getInt32() ? 0 : 1;
            }
            
        },
        new MMinMax ("_max", MNumeric.INT) {
            @Override
            protected int getIndex(PValueSource first,PValueSource second) {
                return first.getInt64() > second.getInt64() ? 0 : 1;
            }
        },
        new MMinMax ("_max", MApproximateNumber.DOUBLE) {
            @Override
            protected int getIndex(PValueSource first,PValueSource second) {
                return first.getDouble() > second.getDouble() ? 0 : 1;
            }
        },
        new MMinMax ("_max", MString.VARCHAR) {
            @Override
            protected int getIndex (PValueSource first, PValueSource second) {
                return first.getString().compareTo(second.getString()) > 0 ? 0 : 1;
            }
        },
    };

    private final String name;
    protected final TClass inputType;

    
    private MMinMax (String name, TClass returnType) {
        this.name = name;
        this.inputType = returnType;
    }

    
    @Override
    public String displayName() {
        return name;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(inputType);
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(inputType, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends PValueSource> inputs, PValueTarget output) {
        int index = getIndex (inputs.get(0), inputs.get(1));
        PValueTargets.copyFrom(inputs.get(index), output);
    }
    
    protected abstract int getIndex(PValueSource first, PValueSource second);
}
