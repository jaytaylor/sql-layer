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

package com.foundationdb.server.store;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.error.FDBAdapterException;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.test.it.FDBITBase;
import com.foundationdb.tuple.Tuple;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FDBExternalDataChangeIT extends FDBITBase
{
    private static final byte[] BAD_PACKED_VALUE = Tuple.from(Long.MIN_VALUE).pack();

    // TODO: Remove when clear support is gone (post 1.9.2)
    // Until then, convenient place to test it

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String,String> map = new HashMap<>(super.startupConfigProperties());
        map.put(FDBSchemaManager.CLEAR_INCOMPATIBLE_DATA_PROP, "true");
        return map;
    }

    protected Map<String, String> defaultPropertiesToPreserveOnRestart() {
        Map<String,String> map = new HashMap<>(super.defaultPropertiesToPreserveOnRestart());
        map.putAll(startupConfigProperties());
        return map;
    }

    @Test
    public void autoClearIncompatible() throws Exception {
        // Store some real data
        int tid = createTable("schema", "test", "id int");
        // Break the data
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                fdbTxnService().getTransaction(session()).setBytes(fdbSchemaManager().getPackedMetaVerKey(), BAD_PACKED_VALUE);
            }
        });
        safeRestartTestServices();
        // Check that the previous data is gone
        assertEquals("table", null, ais().getTable(tid));
    }

    @Test
    public void generationCleared() {
        testClear(fdbSchemaManager().getPackedGenKey());
    }

    @Test
    public void dataCleared() {
        testClear(fdbSchemaManager().getPackedDataVerKey());
    }

    @Test
    public void metaCleared() {
        testClear(fdbSchemaManager().getPackedMetaVerKey());
    }

    @Test
    public void dataVersionChange() {
        testChange(fdbSchemaManager().getPackedDataVerKey());
    }

    @Test
    public void metaChange() {
        testChange(fdbSchemaManager().getPackedMetaVerKey());
    }

    private void testClear(final byte[] key) {
        test(key, null, FDBSchemaManager.EXTERNAL_CLEAR_MSG);
    }

    private void testChange(final byte[] key) {
        test(key, BAD_PACKED_VALUE, FDBSchemaManager.EXTERNAL_VER_CHANGE_MSG);
    }

    private void test(final byte[] key, final byte[] newValue, final String expectedMsg) {
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                TransactionState txn = fdbTxnService().getTransaction(session());
                byte[] prev = txn.getTransaction().get(key).get();
                if(newValue == null) {
                    txn.getTransaction().clear(key);
                } else {
                    txn.setBytes(key, newValue);
                }
                try {
                    fdbSchemaManager().getAis(session());
                    fail("expected exception");
                } catch(FDBAdapterException e) {
                    if(!e.getMessage().contains(expectedMsg)) {
                        assertEquals("exception message", expectedMsg, e.getMessage());
                    }
                }
                txn.getTransaction().set(key, prev);
            }
        });
    }
}
