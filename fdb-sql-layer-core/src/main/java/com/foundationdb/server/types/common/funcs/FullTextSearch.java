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

package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class FullTextSearch extends TScalarBase
{
    public static TScalar[] create(TClass booleanType, TClass stringType) {
        return new TScalar[] {
            new FullTextSearch(booleanType, null),
            new FullTextSearch(booleanType, stringType),
        };
    }

    private final TClass booleanType, stringType;

    private FullTextSearch(TClass booleanType, TClass stringType) {
        this.booleanType = booleanType;
        this.stringType = stringType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        if (stringType == null)
            builder.covers(booleanType, 0);
        else
            builder.covers(stringType, 0).covers(stringType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
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
        return TOverloadResult.fixed(booleanType);
    }
}
