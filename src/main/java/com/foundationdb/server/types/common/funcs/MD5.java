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

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.util.Strings;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5 extends TScalarBase
{
    private final TBinary varbinary;
    private final TString varchar;
    
    public MD5(TBinary varbinary, TString varchar) {
        this.varbinary = varbinary;
        this.varchar = varchar;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(varbinary, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        try
        {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte ret[] = md.digest(inputs.get(0).getBytes());
            
            output.putString(Strings.formatMD5(ret, true), null);
        }
        catch (NoSuchAlgorithmException ex)
        {
            throw new IllegalArgumentException(ex.getMessage());
        }
    }

    @Override
    public String displayName()
    {
        return "md5";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(varchar, 32);
    }
}
