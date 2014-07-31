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
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.async.ReadyFuture;
import com.foundationdb.async.AsyncUtil;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple;
import com.foundationdb.tuple.Tuple2;

/**
 * Represents a potentially large binary value in FoundationDB. For more
 * detailed information, see the synchronized {@link Blob} class, which
 * provides the same functionality in serial. This class, however, runs
 * asynchronously, meaning that its read and write operations can occur
 * in parallel with other operations on the storage substrate without
 * blocking them and allowing the main thread of a program to continue
 * without having to wait for the completion of this class's operations.  
 */
public class BlobAsync {
    // Constants.
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

    private Subspace subspace;

    // Constructor.
    /**
     * Create a new object representing a binary large object (blob).
     * 
     * @param subspace Only keys within the subspace will be used by the object.
     * Other clients of the database should refrain from modifying the subspace. 
     * If there is data already in this subspace in a given database, the blob will 
     * overwrite and may delete this previous data.
     */
    public BlobAsync(Subspace subspace) {
        this.subspace = subspace;
    }

    // Internal class for moving data around within.
    // It is not safe for hash tables. In C/C++, it would be a struct.
    private class Chunk {
        private byte[] key;
        private byte[] data;
        private int startOffset;

        private Chunk() {
            this(null, null, 0);
        }

        private Chunk(byte[] key, byte[] data, int startOffset) {
            this.key = key;
            this.data = data;
            this.startOffset = startOffset;
        }
    }

    // Private functions.

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

    // The key to data "offset" chunks from the beginning of the Blob.
    private byte[] dataKey(int offset) {
        return subspace
                .pack(Tuple2.from(DATA_KEY, String.format("%16d", offset)));
    }
    
    // Given a key to some data, this will return how many chunks from the beginning the data is.
    private int dataKeyOffset(byte[] key) {
        // Gets the last key in the Tuple.
        Tuple t = (Tuple) subspace.unpack(key);
        return Integer.valueOf(t.getString(t.size() - 1).trim());
    }

    // Key to the location of the Blob's size.
    private byte[] sizeKey() {
        return subspace.pack(Tuple2.from(SIZE_KEY));
    }

    // Returns either (key, data, startOffset) or (null, null, 0l).
    private Future<Chunk> getChunkAt(TransactionContext tcx, final int offset) {
        return tcx.runAsync(new Function<Transaction, Future<Chunk>>() {
            @Override
            public Future<Chunk> apply(final Transaction tr) {
                return tr.getRange(subspace.get(DATA_KEY).range(), 1)
                  .asList()
                  .flatMap(new Function<List<KeyValue>, Future<Chunk>>() {
                      @Override
                      public Future<Chunk> apply(final List<KeyValue> kvList) {
                          if(kvList.isEmpty()) {
                              return new ReadyFuture<Chunk>(new Chunk());
                          }
                          KeyValue kv = kvList.get(0);
                          int chunkOffset = dataKeyOffset(kv.getKey());
                          if (chunkOffset + kv.getValue().length <= offset) {
                              // In sparse region after chunk.
                              return new ReadyFuture<>(new Chunk());
                          } else {
                              // Success.
                              return new ReadyFuture<>(new Chunk(kv.getKey(), kv.getValue(), chunkOffset));
                          }
                      }
                  });
            }
        });
    }

    // Splits up the data so that a unit data which we care about that is split
    // across multiple chunks in the blob can be accessed.
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

                        // Set the value at (DATA_KEY, chunk.startOffset) to the
                        // values in
                        // chunk.data[:offset-chunk.startOffset].
                        tr.set(dataKey(chunk.startOffset),
                                Arrays.copyOfRange(chunk.data, 0, splitPoint));

                        // Set the value at (DATA_KEY, offset) to the values in
                        // chunk.data[offset-chunk.startOffset].
                        tr.set(dataKey(offset), Arrays.copyOfRange(chunk.data,
                                splitPoint, chunk.data.length));

                        return null;
                    }
                });
            }
        });
    }

    // Removed data between start and end. It will break up chunks if necessary.
    private Future<Void> makeSparse(TransactionContext tcx, final int start,
            final int end) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(final Transaction tr) {
                /*
                 * Logically, this is equivalent to applying the following
                 * operations in serial:
                 * 
                 * makeSplitPoint(tr, start); 
                 * makeSplitPoint(tr, end);
                 * tr.clear(dataKey(start), dataKey(end)); return null;
                 */
                return makeSplitPoint(tr, start).flatMap(
                        new Function<Void, Future<Void>>() {
                            @Override
                            // Note that arg v1 is not used.
                            public Future<Void> apply(Void v1) {
                                return makeSplitPoint(tr, end).map(
                                        new Function<Void, Void>() {
                                            @Override
                                            // Note that arg v2 is not used.
                                            public Void apply(Void v2) {
                                                tr.clear(dataKey(start),
                                                        dataKey(end));
                                                return null;
                                            }
                                        });
                            }
                        });
            }
        });
    }

    // Return true if split point successfully made and false otherwise.
    private Future<Boolean> tryRemoveSplitPoint(TransactionContext tcx, final int offset) {
        return tcx.runAsync(new Function<Transaction, Future<Boolean>>() {
            @Override
            public Future<Boolean> apply(final Transaction tr) {
                return getChunkAt(tr, offset).flatMap( new Function<Chunk, Future<Boolean>>() {
                    @Override
                    public Future<Boolean> apply(final Chunk bChunk) {
                        if (bChunk.key == null || bChunk.startOffset == 0) {
                            // In sparse region or at beginning.
                            return new ReadyFuture<Boolean>(false);
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

    // This is where the magic really happens--here is where the data is split
    // into chunks and written, chunk by chunk, into the blob.
    private Future<Void> writeToSparse(TransactionContext tcx,
            final int offset, final byte[] data) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(Transaction tr) {
                if (data.length == 0) {
                	// Don't bother writing nothing to the database.
                    return new ReadyFuture<Void>((Void) null); 
                }

                // Determine the number and size of the chunks we will be
                // writing.
                int numChunks = (data.length + CHUNK_LARGE - 1) / (CHUNK_LARGE);
                int chunkSize = (data.length + numChunks) / (numChunks);

                // Write each chunk.
                for (int i = 0; i * chunkSize < data.length; i++) {
                	// Copy over the chunk.
                    int start = i * chunkSize;
                    int end = Math.min((i + 1) * chunkSize, data.length);
                    byte[] chunk = Arrays.copyOfRange(data, start, end); 
                    tr.set(dataKey(start + offset), chunk); // Write it.
                }

                return new ReadyFuture<Void>((Void) null);
            }
        });
    }

    // Sets the value of the size parameter.
    private Future<Void> setSize(TransactionContext tcx, final int size) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(Transaction tr) {
                tr.set(sizeKey(), Tuple2.from(String.valueOf(size)).pack());
                return new ReadyFuture<Void>((Void) null);
            }
        });
    }

    // Public methods
    /**
     * Deletes all key-value pairs associated with the blob so that the
     * information being held is removed from the database. Also, it will remove
     * any data being held in the subspace to which the blob is defined, so if
     * this blob is initialized over a subspace on which previous operations
     * have stored data, this will erase that data.
     * 
     * @param tcx Context to conduct the deletion (typically either the database
     * where the transaction is occurring or another transaction that uses this delete).
     */
    public Future<Void> delete(TransactionContext tcx) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(Transaction tr) {
                // Clears the range covered by the blob.
                tr.clear(subspace.range());
                return new ReadyFuture<Void>((Void)null);
            }
        });
    }

    /**
     * Gets the size of the blob. If there is no size set (or if the size is set
     * to something that is not an integer), then this operation will still
     * succeed, but it will return 0.
     * 
     * @param tcx Context to conduct the transaction (typically either the
     * database where the transaction is occurring or another transaction that uses this one).
     * @return The size the blob or 0 if this information is not accessible.
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
                        // Get the size (which is stored as a string) and returns the
                        // integer case of it.
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
     * @param tcx The context in which to grab the data (usually either the
     * database where the blob is stored or another transaction that needs the 
     * result of this transaction).
     * @param offset The starting position of the read expressed as the bytes from
     * the beginning of the blob.
     * @param n The maximum number of bytes to grab. If the end of the blob is
     * reached during the read, only the rest of the blob will be returned. Otherwise,
     * the next available n bytes are grabbed.
     * @return The data accessed from the blob. This method strings together the
     * raw bytes stored in the database into a larger array of bytes. It is up to the 
     * accessing actor to recreate whatever data underlie these bytes. If there is no 
     * data to read (because <code>offset</code> is greater than the size of the blob, 
     * this returns <code>null</code>.
     */
    public Future<byte[]> read(TransactionContext tcx, final int offset, final int n) {
        /* Logically equivalent to the following serial code:
         * 
         *      // Get the chunks from the database.
         *      AsyncIterable<KeyValue> chunks = tr.getRange(
         *              KeySelector.lastLessOrEqual(dataKey(offset)),
         *              KeySelector.firstGreaterOrEqual(dataKey(offset + n)));
         *      int size = getSize(tr);
         *
         *      if (offset >= size) {
         *          // Gone too far.
         *          return null;
         *      }
         *
         *      // Copy the data over from the database into a byte array.
         *      byte[] result = new byte[(int) Math.min(n, size - (int) offset)];
         *      for (KeyValue chunk : chunks) {
         *          long chunkOffset = dataKeyOffset(chunk.getKey());
         *          for (int i = 0; i < chunk.getValue().length; i++) {
         *              int rPos = (int) (chunkOffset + i - offset);
         *              if (rPos >= 0 && rPos < result.length) {
         *                  result[rPos] = chunk.getValue()[i];
         *              }
         *          }
         *      }
         *
         *      // Return a byte array as taken from the database..
         *      return result;
         */
        return tcx.runAsync(new Function<Transaction, Future<byte[]>>() {
            @Override
            public Future<byte[]> apply(final Transaction tr) {
                return getSize(tr).flatMap(new Function<Integer,Future<byte[]>>() {
                    @Override
                    public Future<byte[]> apply(final Integer size) {
                        if(offset >= size){
                            // Gone too far. Return null.
                            return new ReadyFuture<byte[]>((byte[])null);
                        }
                        
                        // Collect all of the results of the range read taken over the appropriate
                        // range and pack them all together in the same list. 
                        return AsyncUtil.collect(tr.getRange(KeySelector.lastLessOrEqual(dataKey(offset)),
                                KeySelector.firstGreaterOrEqual(dataKey(offset+n)))).map(new Function<List<KeyValue>,byte[]>() {

                            @Override
                            public byte[] apply(List<KeyValue> chunks) {
                                // Copy the data over from the list into a byte array.
                                byte[] result = new byte[(int) Math.min(n, size-offset)];
                                
                                for(KeyValue chunk : chunks){
                                    // Copy this chunk over into the larger array.
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
     * @param tcx The context in which to grab the data (usually either the database where
     * the blob is stored or another transaction that needs the result of this transaction).
     * @return The data contained in the blob. This is expressed as an array of <code>byte</code>s
     * formed by stitching back together the chunked up <code>byte</code>s in the store. It is
     * up to the accessing actor to recreate whatever data underlie these <code>byte</code>s.
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
     * will. It will also overwrite any data that it encounters as it writes, so
     * it is up to the user to be sure there is no data of import where the
     * write is occurring. (Alternatively, this can be used to update old
     * information with new and improved data.) Also, if the end of the blob is
     * encountered during the write, the size of the blob will be increased.
     * 
     * @param tcx The context in which to conduct the write (typically either
     * the database where the data will be written or another transaction that needs 
     * the write to occur).
     * @param offset The place to begin writing expressed as a number of bytes
     * offset from the beginning of the blob.
     * @param data The bytes to write to the blob.
     */
    public Future<Void> write(TransactionContext tcx, final int offset, final byte[] data) {
        /*
         * Logically equivalent to the following serial code:
         * 
         *      if (data.length == 0) {
         *          return null; // Don't bother writing nothing.
         *      }
         *
         *      int end = (int) (offset + data.length);
         *      makeSparse(tr, offset, end);
         *      writeToSparse(tr, offset, data);
         *      tryRemoveSplitPoint(tr, offset);
         *      int oldLength = getSize(tr);
         *
         *      if (end > oldLength) {
         *          setSize(tr, end); // Embiggen file if necessary.
         *      } else {
         *          tryRemoveSplitPoint(tr, end); // Write end needs to be merged.
         *      }
         *      return null;
         */
        return tcx.runAsync(new Function<Transaction,Future<Void>>() {
            @Override
            public Future<Void> apply(final Transaction tr) {
                if(data.length == 0){
                    return new ReadyFuture<Void>((Void)null); // Don't bother writing nothing.
                }
                
                final int end = offset + data.length;
                return makeSparse(tr, offset, end).flatMap(new Function<Void,Future<Void>>() {
                    @Override
                    // Argument v1 not used.
                    public Future<Void> apply(Void v1) {
                        return writeToSparse(tr, offset, data).flatMap(new Function<Void,Future<Void>>() {
                            @Override
                            // Argument v2 not used.
                            public Future<Void> apply(Void v2) {
                                return tryRemoveSplitPoint(tr, offset).flatMap(new Function<Boolean,Future<Void>>() {
                                    @Override
                                    // Argument b1 not used.
                                    public Future<Void> apply(Boolean b1) {
                                        return getSize(tr).flatMap(new Function<Integer,Future<Void>>() {
                                            @Override
                                            public Future<Void> apply(Integer oldLength) {
                                                if(end > oldLength){
                                                    // Embiggen file if necessary.
                                                    return setSize(tr, end); 
                                                } else {
                                                    // Write end needs to be merged.
                                                    return tryRemoveSplitPoint(tr, end).map(new Function<Boolean,Void>() {
                                                        @Override
                                                        // Argument b2 not used.
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
     * @param tcx The context in which to conduct the write (typically either
     * the database where the data will be written or another transaction that 
     * needs this write).
     * @param data The bytes to write to the blob.
     */
    public Future<Void> append(TransactionContext tcx, final byte[] data) {
        /*
         * Logically equivalent to the following serial code:
         * 
         *      write(tr, getSize(tr), data);
         *      return null;
         */
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
     * Changes the blob length to <code>newLength</code>. It erases and data
     * when shrinking, and when enlengthening the blob, the new bytes are filled
     * by zeros.
     * 
     * @param tcx The context in which to truncate the blob (typically either
     * the database where the data will be written or another transaction that needs this truncation).
     * @param newLength The new size of the blob as expressed in bytes.
     */
    public Future<Void> truncate(TransactionContext tcx, final int newLength) {
        /*
         * Logically equivalent to the following serial code:
         * 
         *      makeSparse(tr, newLength, getSize(tr));
         *      setSize(tr, newLength);
         */
        return tcx.runAsync(new Function<Transaction,Future<Void>>() {
            @Override
            public Future<Void> apply(final Transaction tr) {
                return getSize(tr).flatMap(new Function<Integer,Future<Void>>() {
                    @Override
                    public Future<Void> apply(Integer size) {
                        return makeSparse(tr, newLength, size).flatMap(new Function<Void,Future<Void>>() {
                            @Override
                            // Argument v not used.
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
