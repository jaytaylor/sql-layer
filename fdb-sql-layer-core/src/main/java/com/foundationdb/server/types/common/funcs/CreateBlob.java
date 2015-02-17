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


import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.service.blob.LobService;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.Transaction;
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
        BlobRef blob;
        String mode = context.getQueryContext().getStore().getConfig().getProperty(AkBlob.RETURN_UNWRAPPED);
        BlobRef.LeadingBitState state = BlobRef.LeadingBitState.NO;
        byte[] data = new byte[0];
        if (inputs.size() == 1) {
            data = inputs.get(0).getBytes();
        }
        
        if (mode.equalsIgnoreCase(AkBlob.UNWRAPPED)){
            blob = new BlobRef(data, state);
        }
        else {
            state = BlobRef.LeadingBitState.YES;
            ServiceManager sm = context.getQueryContext().getServiceManager();
            TransactionService txnService = context.getQueryContext().getServiceManager().getServiceByClass(TransactionService.class);
            if (txnService instanceof FDBTransactionService) {
                Transaction tr = ((FDBTransactionService) txnService).getTransaction(context.getQueryContext().getStore().getSession()).getTransaction();
                if ((data.length < AkBlob.LOB_SWITCH_SIZE)) {
                    byte[] tmp = new byte[data.length + 1];
                    tmp[0] = BlobRef.SHORT_LOB;
                    System.arraycopy(data, 0, tmp, 1, data.length);
                    data = tmp;
                } 
                else {
                    UUID id = UUID.randomUUID();
                    LobService lobService = sm.getServiceByClass(LobService.class);
                    lobService.createNewLob(tr, id.toString());
                    if (data.length > 0) {
                        lobService.writeBlob(tr, id.toString(), 0, data);
                    }
                    byte[] tmp = new byte[17];
                    tmp[0] = BlobRef.LONG_LOB;
                    System.arraycopy(AkGUID.uuidToBytes(id), 0, tmp, 1, 16);
                    data = tmp;
                }
            }
            blob = new BlobRef(data, state);
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
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkBlob.INSTANCE);
    }
}

