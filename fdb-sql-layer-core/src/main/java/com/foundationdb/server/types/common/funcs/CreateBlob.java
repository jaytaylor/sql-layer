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


import com.foundationdb.server.error.LobException;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.service.blob.LobService;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
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

public class CreateBlob extends TScalarBase {

    public static TScalar createEmptyBlob() {
        return new CreateBlob();
    }

    public static TScalar createBlob(final TClass binaryType) {
        return new CreateBlob() {
            @Override
            protected  void buildInputSets(TInputSetBuilder builder) {
                builder.covers(binaryType, 0);
            }
        };
    }


    private CreateBlob() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        // does nothing
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        UUID id = UUID.randomUUID();
        byte[] data;
        BlobRef blob = new BlobRef(null, new byte[0], BlobRef.SHORT_LOB);
        ServiceManager sm = context.getQueryContext().getServiceManager();
        String blobStorageFormat = sm.getServiceByClass(ConfigurationService.class).getProperty("fdbsql.blob.allowed_storage_format");
        
        if (inputs.size() == 1) {
            data = inputs.get(0).getBytes();
            if ((data.length < AkBlob.LOB_SWITCH_SIZE) && (!blobStorageFormat.equalsIgnoreCase("LONG_LOB")) ) {
                blob = new BlobRef(null, data, BlobRef.SHORT_LOB);
            }
            else if (!blobStorageFormat.equalsIgnoreCase("SHORT_LOB")) {
                LobService lobService = sm.getServiceByClass(LobService.class);
                lobService.createNewLob(id.toString());
                lobService.writeBlob(id.toString(), 0, data);
                blob = new BlobRef(id);

            } else {
                throw new LobException("Lob too large for inline storage");
            }
        }
        output.putObject(blob);
    }

    @Override
    public String displayName() {
        return "CREATE_BLOB";
    }

    @Override
    protected boolean neverConstant() {
        return false;
    }

    @Override
    public String[] registeredNames() {
        return new String[] {"create_blob"};
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkBlob.INSTANCE);
    }
}

