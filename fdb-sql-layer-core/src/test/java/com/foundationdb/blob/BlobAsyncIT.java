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

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BlobAsyncIT extends FDBITBase
{
    @Test
    public void writeReadEmpty() {
        writeAndRead(0);
    }

    @Test
    public void writeReadTiny() {
        writeAndRead(100);
    }

    @Test
    public void writeReadSmall() {
        writeAndRead(1000);
    }

    @Test
    public void writeReadMedium() {
        writeAndRead(10000);
    }

    @Test
    public void writeReadLarge() {
        writeAndRead(100000);
    }

    @Test
    public void writeReadHuge() {
        writeAndRead(1000000);
    }

    @Test
    public void append() {
        final byte[] testBytes1 = generateBytes(100);
        final byte[] testBytes2 = generateBytes(100);
        final byte[] testBytesBoth = ByteArrayUtil.join(testBytes1, testBytes2);
        fdbHolder().getTransactionContext().run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                BlobAsync blob = new BlobAsync(getDir(tr));
                // Append to non-existent
                blob.append(tr, testBytes1).get();
                byte[] readBytes = blob.read(tr).get();
                assertArrayEquals(testBytes1, readBytes);
                // Append to pre-existing
                blob.append(tr, testBytes2).get();
                readBytes = blob.read(tr).get();
                assertArrayEquals(testBytesBoth, readBytes);
                return null;
            }
        });
    }

    @Test
    public void truncate() {
        final byte[] testBytes = generateBytes(100);
        fdbHolder().getTransactionContext().run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                BlobAsync blob = new BlobAsync(getDir(tr));
                blob.write(tr, 0L, testBytes).get();

                blob.truncate(tr, 50).get();
                byte[] readBytes = blob.read(tr).get();
                assertArrayEquals(Arrays.copyOf(testBytes, 50), readBytes);

                blob.truncate(tr, 0).get();
                readBytes = blob.read(tr).get();
                assertNull(readBytes);
                return null;
            }
        });
    }

    @Test
    public void writePartial() {
        final byte[] testBytes = generateBytes(100);
        fdbHolder().getTransactionContext().run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                BlobAsync blob = new BlobAsync(getDir(tr));
                blob.write(tr, 0, testBytes).get();

                byte[] subBytes = new byte[10];
                for(int i = 0; i < subBytes.length; ++i) {
                    subBytes[i] = (byte)(0x42 + i);
                }

                blob.write(tr, 42, subBytes).get();

                byte[] newBytes = Arrays.copyOf(testBytes, testBytes.length);
                System.arraycopy(subBytes, 0, newBytes, 42, subBytes.length);

                byte[] readBytes = blob.read(tr).get();
                assertArrayEquals(newBytes, readBytes);

                return null;
            }
        });
    }

    @Test
    public void readPartial() {
        final byte[] testBytes = generateBytes(100);
        fdbHolder().getTransactionContext().run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                BlobAsync blob = new BlobAsync(getDir(tr));
                blob.write(tr, 0, testBytes).get();

                byte[] subBytes = Arrays.copyOfRange(testBytes, 42, 52);
                byte[] readBytes = blob.read(tr, 42, 10).get();
                assertArrayEquals(subBytes, readBytes);

                return null;
            }
        });
    }

    @Test
    public void scanBounds() {
        final byte[] testBytes = generateBytes(4096);
        fdbHolder().getTransactionContext().run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                DirectorySubspace dir = getDir(tr);
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

    private void writeAndRead(final int len) {
        final byte[] testBytes = generateBytes(len);
        fdbHolder().getTransactionContext().run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                BlobAsync blob = new BlobAsync(getDir(tr));
                blob.write(tr, 0L, testBytes).get();
                assertEquals(Long.valueOf(len), blob.getSize(tr).get());
                byte[] readBytes = blob.read(tr).get();
                if(len == 0) {
                    assertNull(readBytes);
                } else {
                    assertArrayEquals(testBytes, readBytes);
                }
                return null;
            }
        });
    }

    private DirectorySubspace getDir(Transaction tr) {
        DirectorySubspace dir = fdbHolder().getRootDirectory().createOrOpen(tr, PathUtil.from("blob_test")).get();
        tr.clear(dir.range());
        return dir;
    }

    private static byte[] generateBytes(int len) {
        byte[] bytes = new byte[len];
        for(int i = 0; i < len; ++i) {
            bytes[i] = (byte)(i % 10);
        }
        return bytes;
    }
}