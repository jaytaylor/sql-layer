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
import com.foundationdb.server.error.LobException;


public class CreateShortBlob extends TScalarBase {
    public static TScalar createEmptyShortBlob() {
        return new CreateShortBlob();
    }

    public static TScalar createShortBlob(final TClass binaryType) {
        return new CreateShortBlob() {
            @Override
            protected  void buildInputSets(TInputSetBuilder builder) {
                builder.covers(binaryType, 0);
            }
        };
    }


    private CreateShortBlob() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        // does nothing
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        byte[] data = new byte[0];
        if (inputs.size() == 1) {
            data = inputs.get(0).getBytes();
            if ( data.length > AkBlob.LOB_SWITCH_SIZE ) {
                throw new LobException("Lob size too large for small lob");
            }
        }
        BlobRef blob = new BlobRef(null, data, BlobRef.SHORT_LOB);
        output.putObject(blob);
    }

    @Override
    public String displayName() {
        return "CREATE_SHORT_BLOB";
    }

    @Override
    protected boolean neverConstant() {
        return false;
    }

    @Override
    public String[] registeredNames() {
        return new String[] {"create_short_blob"};
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkBlob.INSTANCE);
    }

}
