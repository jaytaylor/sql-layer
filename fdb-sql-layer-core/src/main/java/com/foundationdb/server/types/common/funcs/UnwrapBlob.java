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

import com.foundationdb.server.error.InvalidArgumentTypeException;
import com.foundationdb.server.service.blob.LobService;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.List;


public class UnwrapBlob extends TScalarBase {
    TBinary binaryType;
    
    public static TScalar unwrapBlob(final TBinary binaryType) {
        return new UnwrapBlob(binaryType);
    }


    private UnwrapBlob(final TBinary binaryType) {
        this.binaryType = binaryType;
    }

    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(AkBlob.INSTANCE, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        byte[] data = new byte[0];
        BlobRef blob;
        if (inputs.size() == 1) {
            if (inputs.get(0).hasAnyValue()) {
                Object o = inputs.get(0).getObject();
                if (o instanceof BlobRef) {
                    blob = (BlobRef) o;
                } else {
                    throw new InvalidArgumentTypeException("Should be a blob column");
                }
                if (blob.isShortLob()) {
                    data = blob.getBytes();
                } else {
                    LobService ls = context.getQueryContext().getServiceManager().getServiceByClass(LobService.class);
                    data = ls.readBlob(blob.getId().toString());
                }
            }
        }
        output.putBytes(data);
    }

    @Override
    public String displayName() {
        return "UNWRAP_BLOB";
    }

    @Override
    protected boolean neverConstant() {
        return false;
    }

    @Override
    public String[] registeredNames() {
        return new String[] {"unwrap_blob"};
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TPreptimeValue preptimeValue = inputs.get(0);
                return binaryType.instance(Integer.MAX_VALUE, preptimeValue.isNullable());
            }
        });
    }

}
