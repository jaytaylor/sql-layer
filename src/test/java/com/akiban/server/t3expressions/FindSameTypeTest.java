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

package com.akiban.server.t3expressions;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;
import java.util.BitSet;
import org.junit.Test;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.common.funcs.NoArgFuncs;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import java.util.Arrays;
import java.util.Collection;
import org.junit.runner.RunWith;

import static com.akiban.server.types3.mcompat.mtypes.MString.*;
import static com.akiban.server.types3.mcompat.mtypes.MNumeric.*;
import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class FindSameTypeTest
{
    private final List<? extends TValidatedOverload> group;
    private final BitSet expected;
    
    public FindSameTypeTest(List<? extends TValidatedOverload> group,
                            BitSet expected)
    {
        this.group = group;
        this.expected = expected;
    }

    @Test
    public void test()
    {
        assertEquals(expected,
                     T3RegistryServiceImpl.ScalarsGroupImpl.doFindSameType(group));
    }
     
    @TestParameters
    public static  Collection<Parameterization> params()
    {
        ParameterizationBuilder p = new ParameterizationBuilder();

        // simple case
        test(p,
             getBitSet(true, true, true, true),
             create(CHAR, VARCHAR, INT, BIGINT));

        // simple case:
        test(p,
             getBitSet(false, true, true),
             create(CHAR, VARCHAR, null),
             create(BIGINT, VARCHAR, null),
             create(INT, VARCHAR, CHAR, null),
             create(CHAR, null)
             );
        
        //FOO(A, B) with FOO(A, A...)
        test(p,
             getBitSet(true, false),
             create(VARCHAR, CHAR, null), // FOO(A, B)
             create(VARCHAR, VARCHAR)    // FOO(A, A...)
             );
        
        // FOO(A...) with FOO(B...) // should return [ false... ]
        test(p,
             null,
             create(VARCHAR),
             create(CHAR));
        
        //FOO(A...) with FOO(A) // should return [ true... ]
        test(p,
             getBitSet(true),
             create(VARCHAR),
             create(VARCHAR, null));
        
        // FOO() should return empty BitSet
        test(p,
             getBitSet(),
             new TValidatedOverload(NoArgFuncs.PI));

        return p.asList();
    }
    
    private static BitSet getBitSet(boolean ... val)
    {
        BitSet ret = new BitSet(val.length);
        for (int n = 0; n < val.length; ++n)
            ret.set(n, val[n]);
        return ret;
    }

    private static void test(ParameterizationBuilder p,
                             BitSet exp,
                             TValidatedOverload ... overloads)
    {
        List<? extends TValidatedOverload> group = Arrays.asList(overloads);
        
        p.add("F(" + group + ") ", group, exp);
    }
    
    static TValidatedOverload create (TClass...args)
    {
        return new TValidatedOverload(new Dummy(args));
    }
    
    private static class Dummy extends TOverloadBase
    {
        private final TClass inputs[];
        
        public Dummy(TClass inputs[])
        {
            this.inputs = inputs;
        }

        @Override
        protected void buildInputSets(TInputSetBuilder builder)
        {
            int limit = inputs.length -1;
            for (int n = 0; n < limit; ++n)
                builder.covers(inputs[n], n);
            
            if (inputs[limit] != null) // if there is vararg
                builder.vararg(inputs[limit]);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            //throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String displayName()
        {
            return "String";
        }

        @Override
        public TOverloadResult resultType()
        {
            return TOverloadResult.fixed(MString.CHAR.instance());
        }
    }

}

