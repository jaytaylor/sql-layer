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

package com.foundationdb.util.layers;

import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.foundationdb.tuple.ByteArrayUtil.join;
import static com.foundationdb.tuple.ByteArrayUtil.printable;
import static com.foundationdb.tuple.ByteArrayUtil.strinc;
import static com.foundationdb.util.layers.DirectorySubspace.tupleStr;
import static com.foundationdb.util.layers.Subspace.EMPTY_BYTES;
import static com.foundationdb.util.layers.Subspace.EMPTY_TUPLE;
import static com.foundationdb.util.layers.Subspace.startsWith;

/**
 * Provides a class for managing directories in FoundationDB.
 *
 * <p>
 *     Directories are a recommended approach for administering layers and
 *     applications. Directories work in conjunction with subspaces. Each layer
 *     or application should create or open at least one directory with which
 *     to manage its subspace(s).
 * </p>
 * <p>
 *     Directories are identified by paths (specified as {@link Tuple}s)
 *     analogous to the paths in a Unix-like file system. Each directory
 *     has an associated subspace that is used to store content. The layer
 *     uses a high-contention allocator to efficiently map each path to a
 *     short prefix for its corresponding subspace.
 * </p>
 * <p>
 *     Directory exposes methods to create, open, move, remove, or list
 *     directories. Creating or opening a directory returns the corresponding
 *     subspace.
 * </p>
 */
public class Directory
{
    private static final byte[] LITTLE_ENDIAN_LONG_ONE = { 1, 0, 0, 0, 0, 0, 0, 0 };
    private static final byte[] DEFAULT_NODE_SUBSPACE_PREFIX =  { (byte)0xFE };
    private static final byte[] HIGH_CONTENTION_KEY = { (byte)'h', (byte)'c', (byte)'a' };
    private static final byte[] LAYER_KEY = { (byte)'l', (byte)'a', (byte)'y', (byte)'e', (byte)'r' };
    private static final long SUB_DIR_KEY = 0;

    private static final Subspace DEFAULT_NODE_SUBSPACE = new Subspace(DEFAULT_NODE_SUBSPACE_PREFIX);
    private static final Subspace DEFAULT_CONTENT_SUBSPACE = new Subspace();


    private final Subspace rootNode;
    private final Subspace nodeSubspace;
    private final Subspace contentSubspace;
    private final HighContentionAllocator allocator;


    public Directory() {
        this(DEFAULT_NODE_SUBSPACE, DEFAULT_CONTENT_SUBSPACE);
    }

    public Directory(Subspace nodeSubspace, Subspace contentSubspace) {
        this.nodeSubspace = nodeSubspace;
        this.contentSubspace = contentSubspace;
        // The root node is the one whose contents are the node subspace
        this.rootNode = nodeSubspace.get(nodeSubspace.getKey());
        this.allocator = new HighContentionAllocator(rootNode.get(HIGH_CONTENTION_KEY));
    }

    public static Directory createWithNodeSubspace(Subspace node_subspace) {
        return new Directory(node_subspace, DEFAULT_CONTENT_SUBSPACE);
    }

    public static Directory createWithContentSubspace(Subspace content_subspace) {
        return new Directory(DEFAULT_NODE_SUBSPACE, content_subspace);
    }

    /**
     * Check if the given path exists.
     */
    public boolean exists(Transaction tr, Tuple path) {
        return findNode(tr, path) != null;
    }

    /**
     * Creates, or opens, the directory with the given path.
     *
     * <p>
     *     See {@link #create(Transaction, Tuple)} and
     *     {@link #open(Transaction, Tuple)} for additional details.
     * </p>
     *
     * @throws NoSuchDirectoryException
     * @throws DirectoryAlreadyExistsException
     */
    public DirectorySubspace createOrOpen(Transaction tr, Tuple path) {
        return createOrOpen(tr, path, null, null);
    }

    /**
     * Creates, or opens, the directory with the given path and associate with
     * <code>layer</code>.
     *
     * <p>
     *     See {@link #create(Transaction, Tuple, byte[])} and
     *     {@link #open(Transaction, Tuple, byte[])} for additional details.
     * </p>
     *
     * @throws NoSuchDirectoryException
     * @throws DirectoryAlreadyExistsException
     */
    public DirectorySubspace createOrOpen(Transaction tr, Tuple path, byte[] layer) {
        return createOrOpen(tr, path, layer, null);
    }

    /**
     * As {@link #createOrOpen(Transaction, Tuple, byte[])} and use
     * <code>prefix</code> for the physical location.
     *
     * <p>
     *     See {@link #create(Transaction, Tuple, byte[], byte[])} and
     *     {@link #open(Transaction, Tuple, byte[])} for additional details.
     * </p>
     *
     * @throws NoSuchDirectoryException
     * @throws DirectoryAlreadyExistsException
     */
    public DirectorySubspace createOrOpen(Transaction tr, Tuple path, byte[] layer, byte[] prefix) {
        return createOrOpenInternal(tr, path, layer, prefix, true, true);
    }

    /**
     * Opens the directory with the given path.
     *
     * <p>
     *     An error is raised if the directory does not exist, or if a layer
     *     was originally associated with the directory.
     * </p>
     *
     * @throws NoSuchDirectoryException
     */
    public DirectorySubspace open(Transaction tr, Tuple path) {
        return open(tr, path, null);
    }

    /**
     * Opens the directory with the given path and layer.
     *
     * * <p>
     *     An error is raised if the directory does not exist, or if a
     *     different layer was originally associated with the directory.
     * </p>
     *
     * @throws NoSuchDirectoryException
     */
    public DirectorySubspace open(Transaction tr, Tuple path, byte[] layer) {
        return createOrOpenInternal(tr, path, layer, null, false, true);
    }

    /**
     * Creates a directory with the given path (creating parent directories
     * if necessary).
     *
     * <p>
     *     An error is raised if the given directory already exists.
     * </p>
     *
     * @throws DirectoryAlreadyExistsException
     */
    public DirectorySubspace create(Transaction tr, Tuple path) {
        return create(tr, path, null, null);
    }

    /**
     * As {@link #create(Transaction, Tuple)} and associate with
     * <code>layer</code>.
     *
     * <p>
     *     If layer is not <code>null</code>, it is recorded with the directory
     *     and will be checked by future calls to open.
     * </p>
     *
     * @throws DirectoryAlreadyExistsException
     */
    public DirectorySubspace create(Transaction tr, Tuple path, byte[] layer) {
        return create(tr, path, layer, null);
    }

    /**
     * As {@link #create(Transaction, Tuple, byte[])} and use
     * <code>prefix</code> for the physical location.
     *
     * <p>
     *     If prefix is not <code>null</code>, the directory is created with
     *     the given physical prefix; otherwise a prefix is allocated
     *     automatically.
     * </p>
     *
     * @throws DirectoryAlreadyExistsException
     */
    public DirectorySubspace create(Transaction tr, Tuple path, byte[] layer, byte[] prefix) {
        return createOrOpenInternal(tr, path, layer, prefix, true, false);
    }

    /**
     * Moves the directory found at <code>oldPath</code> to
     * <code>newPath</code>.
     *
     * <p>
     *     There is no effect on the physical prefix of the given directory, or
     *     on clients that already have the directory open.
     * </p>
     * <p>
     *     An error is raised if <code>oldPath</code> does not exist, a
     *     directory already exists at <code>newPath</code>, or the parent
     *     directory of <code>newPath</code> does not exist.
     * </p>
     *
     * @throws NoSuchDirectoryException
     * @throws DirectoryAlreadyExistsException
     */
    public DirectorySubspace move(Transaction tr, Tuple oldPath, Tuple newPath) {
        if(findNode(tr, newPath) != null) {
            throw new DirectoryAlreadyExistsException(newPath);
        }
        Subspace oldNode = findNode(tr, oldPath);
        if(oldNode == null) {
            throw new NoSuchDirectoryException(oldPath);
        }
        Subspace parentNode = findNode(tr, newPath.popBack());
        if(parentNode == null) {
            throw new NoSuchDirectoryException(newPath.popBack());
        }
        tr.set(
            parentNode.get(SUB_DIR_KEY).get(getLast(newPath)).getKey(),
            contentsOfNode(oldNode, null, null).getKey()
        );
        removeFromParent(tr, oldPath);
        return contentsOfNode(oldNode, newPath, tr.get(oldNode.get(LAYER_KEY).getKey()).get());
    }

    /**
     * Removes the directory, all subdirectories and the contents of all
     * directories therein.
     *
     * <p>
     *     <i>
     *         Warning: Clients that have already opened the directory might
     *         still insert data into its contents after it is removed.
     *    </i>
     * </p>
     *
     * @throws NoSuchDirectoryException
     */
    public void remove(Transaction tr, Tuple path) {
        removeInternal(tr, path, true);
    }

    /**
     * As {@link #remove(Transaction, Tuple)} but do not throw if the path does
     * not actually exist.
     *
     * <p>
     * Equivalent to:
     *
     * <code><pre>
     * if(dir.exists(tr, path)) {
     *     dir.remove(tr, path);
     * }
     * </pre></code>
     * </p>
     */
    public void removeIfExists(Transaction tr, Tuple path) {
        removeInternal(tr, path, false);
    }

    /**
     * List the contents of the root directory.
     */
    public List<Object> list(Transaction tr) {
        return list(tr, EMPTY_TUPLE);
    }

    /**
     * List the contents of the given path.
     *
     * @throws NoSuchDirectoryException
     */
    public List<Object> list(Transaction tr, Tuple path) {
        Subspace node = findNode(tr, path);
        if(node == null) {
            throw new NoSuchDirectoryException(path);
        }
        List<Object> dirNames = new ArrayList<>();
        for(NameAndNode nn : listSubDirs(tr, node)) {
            dirNames.add(nn.name);
        }
        return dirNames;
    }


    //
    // Errors consumers can trigger
    //

    public static class DirectoryException extends RuntimeException {
        public final Tuple path;

        public DirectoryException(String baseMsg, Tuple path) {
            super(baseMsg + ": path=" + tupleStr(path));
            this.path = path;
        }
    }

    public static class NoSuchDirectoryException extends DirectoryException {
        public NoSuchDirectoryException(Tuple path) {
            super("No such directory", path);
        }
    }

    public static class DirectoryAlreadyExistsException extends DirectoryException {
        public DirectoryAlreadyExistsException(Tuple path) {
            super("Directory already exists", path);
        }
    }

    public static class MismatchedLayerException extends DirectoryException {
        public final byte[] stored;
        public final byte[] opened;

        public MismatchedLayerException(Tuple path, byte[] stored, byte[] opened) {
            super("Mismatched layer: stored=" + printable(stored) + ", opened=" + printable(opened), path);
            this.stored = stored;
            this.opened = opened;
        }
    }


    //
    // Internal
    //

    private Subspace nodeWithPrefix(byte[] prefix) {
        if(prefix == null) {
            return null;
        }
        return nodeSubspace.get(prefix);
    }

    private Subspace nodeContainingKey(Transaction tr, byte[] key) {
        // Right now this is only used for _is_prefix_free(), but if we add
        // parent pointers to directory nodes, it could also be used to find a
        // path based on a key.
        if(Subspace.startsWith(nodeSubspace.getKey(), key)) {
            return rootNode;
        }
        for(KeyValue kv : tr.getRange(nodeSubspace.range().begin,
                                      join(nodeSubspace.pack(key), new byte[]{ 0x00 }),
                                      1,
                                      true)) {
            byte[] prevPrefix = nodeSubspace.unpack(kv.getKey()).getBytes(0);
            if(startsWith(key, prevPrefix)) {
                return new Subspace(kv.getKey());
            }
        }
        return null;
    }

    private DirectorySubspace contentsOfNode(Subspace node, Tuple path, byte[] layer) {
        byte[] prefix = nodeSubspace.unpack(node.getKey()).getBytes(0);
        return new DirectorySubspace(path, prefix, this, layer);
    }

    private Subspace findNode(Transaction tr, Tuple path) {
        Subspace n = rootNode;
        for(Object name : path) {
            n = nodeWithPrefix(tr.get(n.get(SUB_DIR_KEY).get(name).getKey()).get());
            if(n == null) {
                return null;
            }
        }
        return n;
    }

    private void removeInternal(Transaction tr, Tuple path, boolean mustExist) {
        Subspace n = findNode(tr, path);
        if(n == null) {
            if(mustExist) {
                throw new NoSuchDirectoryException(path);
            }
        } else {
            removeRecursive(tr, n);
            removeFromParent(tr, path);
        }
    }

    private void removeFromParent(Transaction tr, Tuple path) {
        Subspace parent = findNode(tr, path.popBack());
        tr.clear(parent.get(SUB_DIR_KEY).get(getLast(path)).getKey());
    }

    private void removeRecursive(Transaction tr, Subspace node) {
        for(NameAndNode nn : listSubDirs(tr, node)) {
            removeRecursive(tr, nn.node);
        }
        tr.clear(Range.startsWith(contentsOfNode(node, null, null).getKey()));
        tr.clear(node.range());
    }

    private boolean isPrefixFree(Transaction tr, byte[] prefix) {
        // Returns true if the given prefix does not "intersect" any currently
        // allocated prefix (including the root node). This means that it neither
        // contains any other prefix nor is contained by any other prefix.
        if(prefix == null || prefix.length == 0) {
            return false;
        }
        if(nodeContainingKey(tr, prefix) != null) {
            return false;
        }
        Iterator<KeyValue> it = tr.getRange(nodeSubspace.pack(prefix), nodeSubspace.pack(strinc(prefix)), 1).iterator();
        return !it.hasNext();
    }

    private Iterable<NameAndNode> listSubDirs(Transaction tr, Subspace node) {
        final Subspace sd = node.get(SUB_DIR_KEY);
        final Iterator<KeyValue> it = tr.getRange(sd.range()).iterator();
        return new Iterable<NameAndNode>()
        {
            @Override
            public Iterator<NameAndNode> iterator() {
                return new Iterator<NameAndNode>()
                {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public NameAndNode next() {
                        KeyValue kv = it.next();
                        Tuple unpacked = sd.unpack(kv.getKey());
                        assert unpacked.size() == 1 : tupleStr(unpacked);
                        Object name = unpacked.get(0);
                        Subspace node = nodeWithPrefix(kv.getValue());
                        return new NameAndNode(name, node);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    private DirectorySubspace createOrOpenInternal(Transaction tr,
                                                   Tuple path,
                                                   byte[] layer,
                                                   byte[] prefix,
                                                   boolean allowCreate,
                                                   boolean allowOpen) throws NoSuchDirectoryException, DirectoryAlreadyExistsException {
        // Root directory contains node metadata and so may not be opened.
        if(path == null || path.size() == 0) {
            throw new IllegalArgumentException("The root directory may not be opened.");
        }
        Subspace existingNode = findNode(tr, path);
        if(existingNode != null) {
            if(!allowOpen) {
                throw new DirectoryAlreadyExistsException(path);
            }
            byte[] existingLayer = tr.get(existingNode.get(LAYER_KEY).getKey()).get();
            checkLayer(path, layer, existingLayer);
            return contentsOfNode(existingNode, path, existingLayer);
        } else {
            if(!allowCreate) {
                throw new NoSuchDirectoryException(path);
            }
            if(prefix == null) {
                prefix = allocator.allocate(tr);
            }

            if(!isPrefixFree(tr, prefix)) {
                throw new IllegalStateException("Prefix already in use: " + printable(prefix));
            }

            final Subspace parentNode;
            if(path.size() > 1) {
                parentNode = nodeWithPrefix(createOrOpen(tr, path.popBack()).getKey());
            } else {
                parentNode = rootNode;
            }
            assert parentNode != null : "No parent directory: " + tupleStr(path);

            Subspace node = nodeWithPrefix(prefix);
            tr.set(parentNode.get(SUB_DIR_KEY).get(getLast(path)).getKey(), prefix);
            if(layer != null) {
                tr.set(node.get(LAYER_KEY).getKey(), layer);
            }
            return contentsOfNode(node, path, layer);
        }
    }


    //
    // Helpers
    //

    private static long unpackLittleEndian(byte[] bytes) {
        assert bytes.length == 8;
        int value = 0;
        for(int i = 0; i < 8; ++i) {
            value += (bytes[i] << (i * 8));
        }
        return value;
    }

    private static Object getLast(Tuple t) {
        assert t.size() > 0;
        return t.get(t.size() - 1);
    }

    public static void checkLayer(Tuple path, byte[] stored, byte[] opened) {
        // Note: Stricter than Python directory layer, which skips check if either is null
        if(!Arrays.equals(stored, opened)) {
            throw new MismatchedLayerException(path, stored, opened);
        }
    }

    private static class NameAndNode {
        public final Object name;
        public final Subspace node;

        public NameAndNode(Object name, Subspace node) {
            this.name = name;
            this.node = node;
        }
    }

    private static class HighContentionAllocator
    {
        private final Subspace counters;
        private final Subspace recent;
        private final Random random;

        public HighContentionAllocator(Subspace subspace) {
            this.counters = subspace.get(0);
            this.recent = subspace.get(1);
            this.random = new Random();
        }

        /**
         * Returns a byte string that:
         * <ol>
         *     <li>has never and will never be returned by another call to this method on the same subspace</li>
         *     <li>is nearly as short as possible given the above</li>
         * </ol>
         */
        public byte[] allocate(Transaction tr) {
            long start = 0, count = 0;

            Iterator<KeyValue> it = tr.snapshot().getRange(counters.range(), 1, true).iterator();
            if(it.hasNext()) {
                KeyValue kv = it.next();
                start = counters.unpack(kv.getKey()).getLong(0);
                count = unpackLittleEndian(kv.getValue());
            }

            int window = windowSize(start);
            if((count + 1) * 2 >= window) {
                // Advance the window
                byte[] begin = counters.getKey();
                byte[] end = join(counters.get(start).getKey(), new byte[]{ 0x00 });
                tr.clear(begin, end);
                start += window;
                tr.clear(recent.getKey(), recent.get(start).getKey());
                window = windowSize(start);
            }

            // Increment the allocation count for the current window
            tr.mutate(MutationType.ADD, counters.get(start).getKey(), LITTLE_ENDIAN_LONG_ONE);

            while(true) {
                // As of the snapshot being read from, the window is less than half
                // full, so this should be expected to take 2 tries.  Under high
                // contention (and when the window advances), there is an additional
                // subsequent risk of conflict for this transaction.
                long candidate = start + random.nextInt(window);
                byte[] k = recent.get(candidate).getKey();
                if(tr.get(k).get() == null) {
                    tr.set(k, EMPTY_BYTES);
                    return Tuple.from(candidate).pack();
                }
            }
        }

        private static int windowSize(long start) {
            // Larger window sizes are better for high contention, smaller sizes for
            // keeping the keys small.  But if there are many allocations, the keys
            // can't be too small.  So start small and scale up.  We don't want this
            // to ever get *too* big because we have to store about window_size/2
            // recent items.
            if(start < 255) {
                return 64;
            }
            if(start < 65535) {
                return 1024;
            }
            return 8192;
        }
    }
}