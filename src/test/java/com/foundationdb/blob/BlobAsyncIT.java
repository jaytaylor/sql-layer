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

package com.foundationdb.blob;

import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.directory.PathUtil;
import com.foundationdb.server.test.it.FDBITBase;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.ByteArrayUtil;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class BlobAsyncIT extends FDBITBase
{
    @Test
    public void scanBounds() {
        final byte[] testBytes = new byte[1024];
        for(int i = 0; i < testBytes.length; ++i) {
            testBytes[i] = (byte)(i % 10);
        }

        fdbHolder().getDatabase().run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                DirectorySubspace dir = fdbHolder().getRootDirectory().create(tr, PathUtil.from("blob_test")).get();

                byte[] beforePrefix = ByteArrayUtil.join(dir.pack(), new byte[] { 0x41 });
                byte[] blobPrefix = ByteArrayUtil.join(dir.pack(), new byte[] { 0x42 });
                byte[] afterPrefix = ByteArrayUtil.join(dir.pack(), new byte[] { 0x43 });

                tr.set(beforePrefix, beforePrefix);
                tr.set(afterPrefix, afterPrefix);

                BlobAsync blob = new BlobAsync(new Subspace(blobPrefix));
                blob.write(tr, 0, testBytes).get();

                byte[] readBytes = blob.read(tr).get();
                assertArrayEquals(testBytes, readBytes);

                return null;
            }
        });
    }
}