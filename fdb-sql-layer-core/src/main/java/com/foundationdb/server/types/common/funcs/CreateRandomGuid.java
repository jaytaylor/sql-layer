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
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.UUID;

public class CreateRandomGuid extends TScalarBase {
    
    public CreateRandomGuid() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        // does nothing
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        UUID newGuid = UUID.randomUUID();
        output.putObject(newGuid);
    }

    @Override
    public String displayName() {
        return "CREATE_RANDOM_GUID";
    }
    
    @Override
    protected boolean neverConstant() {
        return true;
    }

    @Override
    public String[] registeredNames() {
        return new String[] {"create_random_guid"};
    }
    
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkGUID.INSTANCE);
    }
}
