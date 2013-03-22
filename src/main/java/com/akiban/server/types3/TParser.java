package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public interface TParser
{
    public void parse (TExecutionContext context, PValueSource in, PValueTarget out);
}
