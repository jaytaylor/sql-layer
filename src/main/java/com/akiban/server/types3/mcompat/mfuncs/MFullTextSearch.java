
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class MFullTextSearch extends TScalarBase
{
    public static final TScalar[] overloads = {
        new MFullTextSearch(true),
        new MFullTextSearch(false),
    };

    private boolean singleArg;

    private MFullTextSearch(boolean singleArg) {
        this.singleArg = singleArg;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        if (singleArg)
            builder.covers(AkBool.INSTANCE, 0);
        else
            builder.covers(MString.VARCHAR, 0).covers(MString.VARCHAR, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
            throw new UnsupportedSQLException("This query is not supported by Akiban, its definition " + 
                                              "is used solely for optimization purposes.");
    }

    @Override
    public String displayName()
    {
        return "full_text_search";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(AkBool.INSTANCE);
    }
}
