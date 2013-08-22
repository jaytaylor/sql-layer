/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types3.mcompat.mfuncs;

import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.TScalar;
import com.foundationdb.server.types3.aksql.aktypes.AkBool;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;

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
            throw new UnsupportedSQLException("This query is not supported, its definition " +
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
