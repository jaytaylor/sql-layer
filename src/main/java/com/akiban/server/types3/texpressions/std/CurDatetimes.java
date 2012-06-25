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

package com.akiban.server.types3.texpressions.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;

public abstract class CurDatetimes implements TEvaluatableExpression
{
    public static TEvaluatableExpression currentDate()
    {
        return new CurDatetimes()
        {
            @Override
            public PValueSource resultValue()
            {
                PValue val = new PValue(PUnderlying.INT_32);
                val.putInt32(MDatetimes.encodeDate(context.getCurrentDate())); // TODO
            }   
        };
    }
    
    public static TEvaluatableExpression currentTime()
    {
        return new CurDatetimes()
        {
            @Override
            public PValueSource resultValue()
            {
                PValue val = new PValue(PUnderlying.INT_32);
                val.putInt32(MDatetimes.encodeTime(context.getCurrentDate())); // TODO
            }   
        };
    }
    
    public static TEvaluatableExpression currentDateTime()
    {
        return new CurDatetimes()
        {
            @Override
            public PValueSource resultValue()
            {
                PValue val = new PValue(PUnderlying.INT_64);
                val.putInt64(MDatetimes.encodeDateTime(context.getCurrentDate())); // TODO
            }   
        };
    }
        
    protected QueryContext context;

    @Override
    public void evaluate()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void with(Row row)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void with(QueryContext context)
    {
        this.context = context;
    }
}
