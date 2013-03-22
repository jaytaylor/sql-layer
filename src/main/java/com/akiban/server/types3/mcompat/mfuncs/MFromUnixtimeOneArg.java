
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class MFromUnixtimeOneArg extends TScalarBase
{
    public static final TScalar INSTANCE = new MFromUnixtimeOneArg();

    private MFromUnixtimeOneArg(){}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.BIGINT, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        // unixtime is in second
        // convert it to millis
        long millis = inputs.get(0).getInt64() * 1000L;
        output.putInt64(MDatetimes.encodeDatetime(
                MDatetimes.fromJodaDatetime(
                    new DateTime(millis, DateTimeZone.forID(context.getCurrentTimezone())))));
    }

    @Override
    public String displayName()
    {
        return "FROM_UNIXTIME";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MDatetimes.DATETIME);
    }
}
