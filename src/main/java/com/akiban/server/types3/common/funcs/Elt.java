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

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class Elt extends TScalarBase
{

    private final TClass stringType;
    private final TClass intType;
    
    public Elt(TClass intType, TClass stringType)
    {
        this.intType = intType;
        this.stringType = stringType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        // ELT(<INT>, <T> ....)
        // argc >= 2
        builder.covers(intType, 0).pickingVararg(stringType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        int index = inputs.get(0).getInt32();
        int nvarargs = inputs.size();
        if (index < 1 || index >= nvarargs)
            output.putNull();
        else
            PValueTargets.copyFrom(inputs.get(index), output);
    }
     
    @Override
    protected boolean constnessMatters(int inputIndex) 
    {
        return true;
    }

    @Override
    protected Constantness constness(int inputIndex, LazyList<? extends TPreptimeValue> values) {
        assert inputIndex == 0 : inputIndex + " for " + values; // 0 should be enough to fully answer the question
        PValueSource indexVal = constSource(values, 0);
        if (indexVal == null)
            return Constantness.NOT_CONST;
        if (indexVal.isNull())
            return Constantness.CONST;
        int answerIndex = indexVal.getInt32();
        if (answerIndex < 1 || answerIndex >= values.size())
            return Constantness.CONST; // answer is null
        PValueSource answer = constSource(values, answerIndex);
        return answer == null ? Constantness.NOT_CONST : Constantness.CONST;
    }

    @Override
    protected boolean nullContaminates(int inputIndex) 
    {
        return inputIndex == 0;
    }

    @Override
    public String displayName()
    {
        return "ELT";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.picking();
    }
}
