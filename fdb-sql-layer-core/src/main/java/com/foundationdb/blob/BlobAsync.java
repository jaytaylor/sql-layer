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

import java.util.Arrays;
import java.util.List;

import com.foundationdb.*;
import com.foundationdb.async.*;
import com.foundationdb.subspace.*;
import com.foundationdb.tuple.*;

/** Represents a potentially large binary value in FoundationDB. */
public class BlobAsync {
    /**
     * The size parameter of the blob is held in the subspace indexed by
     * <code>SIZE_KEY</code> of the blob's main subspace.
     */
    protected static final String SIZE_KEY = "S";
    /**
     * The actual data of the blob is held in the subspace indexed by
     * <code>DATA_KEY</code> of the blob's main subspace.
     */
    protected static final String DATA_KEY = "D";
    /**
     * Certain attributes about the blob can be stored at the subspace indexed
     * by <code>ATTRIBUTE_KEY</code> of the blob's main subspace. This field is
     * not actually used by the class, but subclasses may find it useful to
     * have.
     */
    protected static final String ATTRIBUTE_KEY = "A";
    /**
     * This is the maximum size of a chunk within the blob. No chunks will ever
     * be greater than it.
     */
    protected static final int CHUNK_LARGE = 10000;
    /**
     * This field is used internally and represents the good practice chunk
     * size--that is, chunks below this size will be combined with other chunks
     * while those larger will not.
     */
    protected static final int CHUNK_SMALL = 200;

    private final Subspace subspace;

    /**
     * Create a new object representing a binary large object (blob).
     *
     * @param subspace Subspace to use to write data. Should be considered owned by the blob.
     */
    public BlobAsync(Subspace subspace) {
        this.subspace = subspace;
    }

    private static class Chunk {
        private final byte[] key;
        private final byte[] data;
        private final int startOffset;

        private Chunk() {
            this(null, null, 0);
        }

        private Chunk(byte[] key, byte[] data, int startOffset) {
            this.key = key;
            this.data = data;
            this.startOffset = startOffset;
        }
    }

    /**
     * Gets the location of whatever attributes are stored about the blob. This
     * is mainly included for inheritance purposes as subclasses may use the
     * attribute field, though the vanilla blob does not use this function.
     *
     * @return The location of the attributes of the blob.
     */
    protected byte[] attributeKey() {
        return subspace.pack(Tuple2.from(ATTRIBUTE_KEY));
    }

    /** The key to data "offset" chunks from the beginning of the Blob. */
    private byte[] dataKey(int offset) {
        return subspace.pack(Tuple2.from(DATA_KEY, String.format("%16d", offset)));
    }

    /** Given a key to some data, this will return how many chunks from the beginning the data is. **/
    private int dataKeyOffset(byte[] key) {
        Tuple t = subspace.unpack(key);
        return Integer.valueOf(t.getString(t.size() - 1).trim());
    }

    /** Key to the location of the Blob's size. */
    private byte[] sizeKey() {
        return subspace.pack(Tuple2.from(SIZE_KEY));
    }

    /** Returns either (key, data, startOffset) or (null, null, 0) */
    private Future<Chunk> getChunkAt(TransactionContext tcx, final int offset) {
        return tcx.runAsync(new Function<Transaction, Future<Chunk>>() {
            @Override
            public Future<Chunk> apply(final Transaction tr) {
                return tr.getKey(KeySelector.lastLessOrEqual(dataKey(offset)))
                         .flatMap(new Function<byte[], Future<Chunk>>() {
                             @Override
                             public Future<Chunk> apply(final byte[] chunkKey) {
                                 // Nothing before (sparse) or before beginning
                                 if ((chunkKey == null) ||
                                     (ByteArrayUtil.compareUnsigned(chunkKey, dataKey(0)) < 0)) {
                                     return new ReadyFuture<>(new Chunk());
                                 }
                                 final int chunkOffset = dataKeyOffset(chunkKey);
                                 return tr.get(chunkKey).map(
                                     new Function<byte[], Chunk>() {
                                         @Override
                                         public Chunk apply(byte[] chunkData) {
                                             if (chunkOffset + chunkData.length <= offset) {
                                                 // In sparse region after chunk.
                                                 return new Chunk();
                                             }
                                             // Success.
                                             return new Chunk(chunkKey, chunkData, chunkOffset);
                                         }
                                     });
                             }
                         });
            }
        });
    }

    /**
     * Splits up the data so that a unit of data which we care about that is split
     * across multiple chunks in the blob can be accessed.
     */
    private Future<Void> makeSplitPoint(TransactionContext tcx, final int offset) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(final Transaction tr) {
                return getChunkAt(tr, offset).map(new Function<Chunk, Void>() {
                    @Override
                    public Void apply(Chunk chunk) {
                        if (chunk.key == null) {
                            return null; // Already sparse.
                        }
                        if (chunk.startOffset == offset) {
                            return null; // Already a split point.
                        }

                        int splitPoint = offset - chunk.startOffset;

                        // Set the value at (DATA_KEY, chunk.startOffset) to the values in
                        // chunk.data[:offset-chunk.startOffset].
                        tr.set(dataKey(chunk.startOffset),
                                Arrays.copyOfRange(chunk.data, 0, splitPoint));

                        // Set the value at (DATA_KEY, offset) to the values in
                        // chunk.data[offset-chunk.startOffset].
                        tr.set(dataKey(offset),
                               Arrays.copyOfRange(chunk.data, splitPoint, chunk.data.length));

                        return null;
                    }
                });
            }
        });
    }

    /** Removed data between start and end. It will break up chunks if necessary. */
    private Future<Void> makeSparse(TransactionContext tcx, final int start, final int end) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(final Transaction tr) {
                return makeSplitPoint(tr, start).flatMap(
                    new Function<Void, Future<Void>>() {
                        @Override
                        public Future<Void> apply(Void v1) {
                            return makeSplitPoint(tr, end).map(
                                new Function<Void, Void>() {
                                    @Override
                                    public Void apply(Void v2) {
                                        tr.clear(dataKey(start), dataKey(end));
                                        return null;
                                    }
                                });
                        }
                    });
            }
        });
    }

    /** Return true if split point successfully made and false otherwise. */
    private Future<Boolean> tryRemoveSplitPoint(TransactionContext tcx, final int offset) {
        return tcx.runAsync(new Function<Transaction, Future<Boolean>>() {
            @Override
            public Future<Boolean> apply(final Transaction tr) {
                return getChunkAt(tr, offset).flatMap( new Function<Chunk, Future<Boolean>>() {
                    @Override
                    public Future<Boolean> apply(final Chunk bChunk) {
                        if (bChunk.key == null || bChunk.startOffset == 0) {
                            // In sparse region or at beginning.
                            return new ReadyFuture<>(false);
                        }
                        return getChunkAt(tr, bChunk.startOffset - 1).map(new Function<Chunk, Boolean>() {
                            @Override
                            public Boolean apply(Chunk aChunk) {
                                if (aChunk.key == null) {
                                    return false; // No previous chunk.
                                }

                                if (aChunk.startOffset + aChunk.data.length != bChunk.startOffset) {
                                    return false; // Chunks can't be joined.
                                }

                                if (aChunk.data.length + bChunk.data.length > CHUNK_SMALL) {
                                    return false; // Chunks shouldn't be joined.
                                }

                                // We can merge chunks!
                                tr.clear(bChunk.key);
                                byte[] joined = new byte[aChunk.data.length + bChunk.data.length];
                                System.arraycopy(aChunk.data, 0, joined, 0, aChunk.data.length);
                                System.arraycopy(bChunk.data, 0, joined, aChunk.data.length, bChunk.data.length);
                                tr.set(aChunk.key, joined);

                                return true;
                            }
                        });
                    }
                });
            }
        });
    }

    /** Split data into chunks and write into the blob. */
    private Future<Void> writeToSparse(TransactionContext tcx, final int offset, final byte[] data) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(Transaction tr) {
                if (data.length == 0) {
                	// Don't bother writing nothing to the database.
                    return new ReadyFuture<>((Void)null);
                }

                // Determine the number and size of the chunks we will be writing.
                int numChunks = (data.length + CHUNK_LARGE - 1) / (CHUNK_LARGE);
                int chunkSize = (data.length + numChunks) / (numChunks);

                for (int i = 0; i * chunkSize < data.length; i++) {
                    int start = i * chunkSize;
                    int end = Math.min((i + 1) * chunkSize, data.length);
                    byte[] chunk = Arrays.copyOfRange(data, start, end);
                    tr.set(dataKey(start + offset), chunk); // Write it.
                }

                return new ReadyFuture<>((Void)null);
            }
        });
    }

    /** Sets the value of the size parameter. Does not change any blob data. */
    private Future<Void> setSize(TransactionContext tcx, final int size) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(Transaction tr) {
                tr.set(sizeKey(), Tuple2.from(String.valueOf(size)).pack());
                return new ReadyFuture<>((Void) null);
            }
        });
    }


    /**
     * Deletes all key-value pairs associated with the blob by
     * <b>clearing the underlying subspace</b>.
     *
     * @param tcx Context to conduct the deletion
     */
    public Future<Void> delete(TransactionContext tcx) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(Transaction tr) {
                tr.clear(subspace.range());
                return new ReadyFuture<>((Void)null);
            }
        });
    }

    /**
     * Gets the size of the blob.
     *
     * @param tcx Context to conduct the transaction
     * @return The size the blob or 0 if no size is set.
     */
    public Future<Integer> getSize(TransactionContext tcx) {
        return tcx.runAsync(new Function<Transaction, Future<Integer>>() {
            @Override
            public Future<Integer> apply(Transaction tr) {
                return tr.get(sizeKey()).map(new Function<byte[],Integer>() {
                    @Override
                    public Integer apply(byte[] sizeBytes) {
                        if(sizeBytes == null) {
                            return 0;
                        }
                        String sizeStr = Tuple2.fromBytes(sizeBytes).getString(0);
                        return Integer.valueOf(sizeStr);
                    }
                });
            }
        });
    }

    /**
     * Reads from the blob a certain number of bytes and returns them in a
     * single array. Essentially, this method reconstitutes a certain subset of
     * the stored blob and presents in the way the user expects.
     *
     * @param tcx The context in which to grab the data
     * @param offset The starting position of the read expressed as the bytes from
     * the beginning of the blob.
     * @param n The maximum number of bytes to grab. If the end of the blob is
     * reached during the read, only the rest of the blob will be returned. Otherwise,
     * the next available n bytes are grabbed.
     *
     * @return The data accessed from the blob. <code>null</code> is returned if
     * there is no data to read.
     */
    public Future<byte[]> read(TransactionContext tcx, final int offset, final int n) {
        return tcx.runAsync(new Function<Transaction, Future<byte[]>>() {
            @Override
            public Future<byte[]> apply(final Transaction tr) {
                return getSize(tr).flatMap(new Function<Integer,Future<byte[]>>() {
                    @Override
                    public Future<byte[]> apply(final Integer size) {
                        if(offset >= size){
                            // Gone too far. Return null.
                            return new ReadyFuture<>((byte[])null);
                        }
                        // Collect all of the results of the range read taken over the appropriate
                        // range and pack them all together in the same list.
                        return tr.getRange(KeySelector.lastLessOrEqual(dataKey(offset)),
                                           KeySelector.firstGreaterOrEqual(dataKey(offset+n)))
                                 .asList()
                                 .map(new Function<List<KeyValue>,byte[]>() {
                                     @Override
                                     public byte[] apply(List<KeyValue> chunks) {
                                         // Copy the data over from the list into a byte array.
                                         byte[] result = new byte[Math.min(n, size-offset)];
                                         for(KeyValue chunk : chunks){
                                             long chunkOffset = dataKeyOffset(chunk.getKey());
                                             for(int i = 0; i < chunk.getValue().length; i++){
                                                 int rPos = (int)(chunkOffset + i -offset);
                                                 if(rPos >= 0 && rPos < result.length) {
                                                     result[rPos] = chunk.getValue()[i];
                                                 }
                                             }
                                         }
                                         return result;
                                     }
                                 });
                    }
                });
            }
        });
    }

    /**
     * Reads and returns the entire blob. 
     * 
     * @param tcx The context in which to grab the data
     * @return The data contained in the blob.
     */
    public Future<byte[]> read(TransactionContext tcx) {
        return tcx.runAsync(new Function<Transaction,Future<byte[]>>() {
            @Override
            public Future<byte[]> apply(final Transaction tr) {
                return getSize(tr).flatMap(new Function<Integer,Future<byte[]>>() {
                    @Override
                    public Future<byte[]> apply(Integer size) {
                        return read(tr, 0, size);
                    }
                });
            }
        });
    }

    /**
     * Writes <code>data</code> to the database starting at <code>offset</code>.
     * It will break <code>data</code> into chunks as necessary by its size and
     * will. It will also overwrite any data that it encounters as it writes.
     * 
     * @param tcx The context in which to conduct the write.
     * @param offset The place to begin writing expressed as a number of bytes
     * offset from the beginning of the blob.
     * @param data The bytes to write to the blob.
     */
    public Future<Void> write(TransactionContext tcx, final int offset, final byte[] data) {
         return tcx.runAsync(new Function<Transaction,Future<Void>>() {
            @Override
            public Future<Void> apply(final Transaction tr) {
                if(data.length == 0){
                    return new ReadyFuture<>((Void)null); // Don't bother writing nothing.
                }
                
                final int end = offset + data.length;
                return makeSparse(tr, offset, end).flatMap(new Function<Void,Future<Void>>() {
                    @Override
                    public Future<Void> apply(Void v1) {
                        return writeToSparse(tr, offset, data).flatMap(new Function<Void,Future<Void>>() {
                            @Override
                            public Future<Void> apply(Void v2) {
                                return tryRemoveSplitPoint(tr, offset).flatMap(new Function<Boolean,Future<Void>>() {
                                    @Override
                                    public Future<Void> apply(Boolean b1) {
                                        return getSize(tr).flatMap(new Function<Integer,Future<Void>>() {
                                            @Override
                                            public Future<Void> apply(Integer oldLength) {
                                                if(end > oldLength){
                                                    // Lengthen if necessary.
                                                    return setSize(tr, end); 
                                                } else {
                                                    // Write end needs to be merged.
                                                    return tryRemoveSplitPoint(tr, end).map(new Function<Boolean,Void>() {
                                                        @Override
                                                        public Void apply(Boolean b2) {
                                                            return null;
                                                        }
                                                    });
                                                }
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });
    }

    /**
     * Appends the contents of <code>data</code> onto the end of the blob.
     * 
     * @param tcx The context in which to conduct the write
     * @param data The bytes to write to the blob.
     */
    public Future<Void> append(TransactionContext tcx, final byte[] data) {
        return tcx.runAsync(new Function<Transaction,Future<Void>>() {
            @Override
            public Future<Void> apply(final Transaction tr) {
                return getSize(tr).flatMap(new Function<Integer,Future<Void>>() {
                    @Override
                    public Future<Void> apply(Integer size) {
                        return write(tr, size, data);
                    }
                });
            }
        });
    }

    /**
     * Changes the blob length to <code>newLength</code>. It erases the data
     * when shrinking, and when lengthening the blob, the new bytes are filled
     * by zeros.
     * 
     * @param tcx The context in which to truncate the blob.
     * @param newLength The new size of the blob as expressed in bytes.
     */
    public Future<Void> truncate(TransactionContext tcx, final int newLength) {
        return tcx.runAsync(new Function<Transaction,Future<Void>>() {
            @Override
            public Future<Void> apply(final Transaction tr) {
                return getSize(tr).flatMap(new Function<Integer,Future<Void>>() {
                    @Override
                    public Future<Void> apply(Integer size) {
                        return makeSparse(tr, newLength, size).flatMap(new Function<Void,Future<Void>>() {
                            @Override
                            public Future<Void> apply(Void v) {
                                return setSize(tr, newLength);
                            }
                        });
                    }
                });
            }
        });
    }
}
