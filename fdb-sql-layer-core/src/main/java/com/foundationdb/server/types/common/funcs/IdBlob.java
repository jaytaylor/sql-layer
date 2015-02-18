/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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


import com.foundationdb.server.error.InvalidArgumentTypeException;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.UUID;

public class IdBlob extends TScalarBase {
    NoAttrTClass blob;

    public static TScalar idBlob(final NoAttrTClass blob) {
        return new IdBlob(blob);
    }


    private IdBlob(NoAttrTClass blob) {
        this.blob = blob;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(blob, 0); 
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        UUID guid = null;
        BlobRef blob;
        if (inputs.size() == 1) {
            if (inputs.get(0).hasAnyValue()) {
                Object o = inputs.get(0).getObject();
                if (o instanceof BlobRef) {
                    blob = (BlobRef) o;
                    if (blob.isLongLob()) {
                        guid = blob.getId();
                    }
                } else {
                    throw new InvalidArgumentTypeException("Should be a blob column");
                }

            }
        }
        output.putObject(guid);
    }

    @Override
    public String displayName() {
        return "ID_BLOB";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkGUID.INSTANCE);
    }
}


