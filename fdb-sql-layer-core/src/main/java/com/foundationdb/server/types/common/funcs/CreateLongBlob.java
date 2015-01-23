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


import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.service.blob.LobService;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.UUID;

public class CreateLongBlob extends TScalarBase {
    
    public static TScalar createEmptyLongBlob() {
        return new CreateLongBlob();
    }
    
    public static TScalar createLongBlob(final TClass binaryType) {
        return new CreateLongBlob() {
            @Override
            protected  void buildInputSets(TInputSetBuilder builder) {
                builder.covers(binaryType, 0);
            }
        };
    }
    
    
    private CreateLongBlob() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        // does nothing
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        UUID id = UUID.randomUUID();
        byte[] data;
        LobService lobService = context.getQueryContext().getServiceManager().getServiceByClass(LobService.class);
        lobService.createNewLob(id.toString());
        if (inputs.size() == 1) {
            data = inputs.get(0).getBytes();
            if (data.length > 0) {
                lobService.writeBlob(id.toString(), 0, data);
            }
        }
        BlobRef blob = new BlobRef(id);
        output.putObject(blob);
    }

    @Override
    public String displayName() {
        return "CREATE_LONG_BLOB";
    }

    @Override
    protected boolean neverConstant() {
        return true;
    }

    @Override
    public String[] registeredNames() {
        return new String[] {"create_long_blob"};
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkBlob.INSTANCE);
    }
}

