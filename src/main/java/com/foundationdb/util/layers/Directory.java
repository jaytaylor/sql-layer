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

import static com.foundationdb.util.layers.Subspace.EMPTY_BYTES;
import static com.foundationdb.util.layers.Subspace.startsWith;
import static com.foundationdb.tuple.ByteArrayUtil.join;
import static com.foundationdb.tuple.ByteArrayUtil.printable;
import static com.foundationdb.tuple.ByteArrayUtil.strinc;

/**
 * Provides a DirectoryLayer class for managing directories in FoundationDB.
 * Directories are a recommended approach for administering layers and
 * applications. Directories work in conjunction with subspaces. Each layer or
 * application should create or open at least one directory with which to manage
 * its subspace(s).
 *
 * Directories are identified by paths (specified as tuples) analogous to the paths
 * in a Unix-like file system. Each directory has an associated subspace that is
 * used to store content. The layer uses a high-contention allocator to efficiently
 * map each path to a short prefix for its corresponding subspace.
 *
 * DirectorLayer exposes methods to create, open, move, remove, or list
 * directories. Creating or opening a directory returns the corresponding subspace.
 *
 * The DirectorySubspace class represents subspaces that store the contents of a
 * directory. An instance of DirectorySubspace can be used for all the usual
 * subspace operations. It can also be used to operate on the directory with which
 * it was opened.
 */
public class Directory
{
    private static final Subspace DEFAULT_NODE_SUBSPACE = new Subspace(new byte[]{ (byte)254 });
    private static final Subspace DEFAULT_CONTENT_SUBSPACE = new Subspace();
    private static final byte[] LITTLE_ENDIAN_LONG_ONE = { 1, 0, 0, 0, 0, 0, 0, 0 };
    private static final long SUBDIRS = 0;

    private final Subspace node_subspace;
    private final Subspace content_subspace;
    private final Subspace root_node;
    private final HighContentionAllocator allocator;


    public Directory() {
        this(DEFAULT_NODE_SUBSPACE, DEFAULT_CONTENT_SUBSPACE);
    }

    public Directory(Subspace node_subspace, Subspace content_subspace) {
        this.node_subspace = node_subspace;
        this.content_subspace = content_subspace;
        // The root node is the one whose contents are the node subspace
        this.root_node = node_subspace.get(node_subspace.getKey());
        this.allocator = new HighContentionAllocator(root_node.get("hca"));
    }

    public static Directory createWithNodeSubspace(Subspace node_subspace) {
        return new Directory(node_subspace, DEFAULT_CONTENT_SUBSPACE);
    }

    public static Directory createWithContentSubspace(Subspace content_subspace) {
        return new Directory(DEFAULT_NODE_SUBSPACE, content_subspace);
    }


    /**
     * Opens the directory with the given path.
     *
     * If the directory does not exist, it is created (creating parent
     * directories if necessary).
     *
     * If prefix is specified, the directory is created with the given physical
     * prefix; otherwise a prefix is allocated automatically.
     *
     * If layer is specified, it is checked against the layer of an existing
     * directory or set as the layer of a new directory.
     */
    public DirectorySubspace create_or_open(Transaction tr,
                                            Tuple path,
                                            byte[] layer/*=None*/,
                                            byte[] prefix/*=None*/,
                                            boolean allow_create/*=True*/,
                                            boolean allow_open/*=True*/) {
        if(path == null || path.size() == 0) {
            // Root directory contains node metadata and so may not be opened.
            throw new IllegalArgumentException("The root directory may not be opened.");
        }
        Subspace existing_node = _find(tr, path);
        if(existing_node != null) {
            if(!allow_open) {
                throw new IllegalArgumentException("The directory already exists.");
            }
            byte[] existing_layer = tr.get( existing_node.get("layer").getKey() ).get();
            if(layer != null && existing_layer != null && !Arrays.equals(existing_layer, layer)) {
                throw new IllegalArgumentException("The directory exists but was created with an incompatible layer.");
            }
            return _contents_of_node(existing_node, path, existing_layer);
        }
        if(!allow_create) {
            throw new IllegalArgumentException("The directory does not exist.");
        }

        if(prefix == null) {
            prefix = allocator.allocate(tr);
        }

        if(!_is_prefix_free(tr, prefix)) {
            throw new IllegalArgumentException("The given prefix is already in use.");
        }

        final Subspace parent_node;
        if(path.size() > 1) {
            parent_node = _node_with_prefix(create_or_open(tr, removeLast(path), null, null, true, true).getKey());
        } else {
            parent_node = root_node;
        }

        if(parent_node == null) {
            //print repr(path[:-1])
            throw new IllegalArgumentException("The parent directory doesn't exist.");
        }

        Subspace node = _node_with_prefix(prefix);
        tr.set( parent_node.get(SUBDIRS).get( path.get(path.size() - 1) ).getKey(), prefix);
        if(layer != null) {
            tr.set( node.get("layer").getKey(), layer);
        }

        return _contents_of_node(node, path, layer);
    }

    /**
     * Opens the directory with the given path.
     *
     * An error is raised if the directory does not exist, or if a layer is
     * specified and a different layer was specified when the directory was
     * created.
     */
    public DirectorySubspace open(Transaction tr, Tuple path, byte[] layer/*=None*/) {
        return create_or_open(tr, path, layer, null, false, true);
    }

    /**
     * Creates a directory with the given path (creating parent directories
     * if necessary).
     *
     * An error is raised if the given directory already exists.
     *
     * If prefix is specified, the directory is created with the given physical
     * prefix; otherwise a prefix is allocated automatically.
     *
     * If layer is specified, it is recorded with the directory and will be
     * checked by future calls to open.
     */
    public DirectorySubspace create(Transaction tr, Tuple path, byte[] layer/*=None*/, byte[] prefix/*=None*/) {
        return create_or_open(tr, path, layer, prefix, true, false);
    }

    /**
     *
     *  Moves the directory found at `old_path` to `new_path`.
     *
     * There is no effect on the physical prefix of the given directory, or on
     * clients that already have the directory open.
     *
     * An error is raised if the old directory does not exist, a directory
     * already exists at `new_path`, or the parent directory of `new_path` does
     * not exist.
     */
    public DirectorySubspace move(Transaction tr, Tuple old_path, Tuple new_path) {
        if(_find(tr, new_path) != null) {
            throw new IllegalArgumentException("The destination directory already exists. Remove it first.");
        }
        Subspace old_node = _find(tr, old_path);
        if(old_node == null) {
            throw new IllegalArgumentException("The source directory does not exist.");
        }
        Subspace parent_node = _find(tr, removeLast(new_path));
        if(parent_node == null) {
            throw new IllegalArgumentException("The parent of the destination directory does not exist. Create it first.");
        }
        tr.set(parent_node.get(SUBDIRS).get(new_path.get(new_path.size() - 1)).getKey(),_contents_of_node(old_node, null, null).getKey());
        _remove_from_parent(tr, old_path);
        return _contents_of_node(old_node, new_path, tr.get(old_node.get("layer").getKey()).get());
    }

    /**
     * Removes the directory, its contents, and all subdirectories.
     *
     * Warning: Clients that have already opened the directory might still
     * insert data into its contents after it is removed.
     */
    //@fdb.transactional
    public void remove(Transaction tr, Tuple path) {
        Subspace n = _find(tr, path);
        if(n == null) {
            throw new IllegalArgumentException("The directory doesn't exist.");
        }
        _remove_recursive(tr, n);
        _remove_from_parent(tr, path);
    }

    //@fdb.transactional
    public List<byte[]> list(Transaction tr, Tuple path/*=()*/) {
        Subspace node = _find(tr, path);
        if(node == null) {
            throw new IllegalArgumentException("The given directory does not exist.");
        }
        List<byte[]> dirNames = new ArrayList<>();
        for(NameAndNode nn : _subdir_names_and_nodes(tr, node)) {
            dirNames.add(nn.name);
        }
        return dirNames;
    }


    //
    // Internal
    //

    private Subspace _node_containing_key(Transaction tr, byte[] key) {
        // Right now this is only used for _is_prefix_free(), but if we add
        // parent pointers to directory nodes, it could also be used to find a
        // path based on a key.
        if(Subspace.startsWith(node_subspace.getKey(), key)) {
            return root_node;
        }
        for(KeyValue kv : tr.getRange(node_subspace.range().begin,
                                      join(node_subspace.pack(key), new byte[]{0x00}),
                                      1,
                                      true)) {
            byte[] prev_prefix = node_subspace.unpack(kv.getKey()).getBytes(0);
            if(startsWith(key, prev_prefix)) {
                return new Subspace(kv.getKey());  // self.node_subspace[prev_prefix]
            }
        }
        return null;
    }

    private Subspace _node_with_prefix(byte[] prefix) {
        if(prefix == null) {
            return null;
        }
        return node_subspace.get(prefix);
    }

    private DirectorySubspace _contents_of_node(Subspace node, Tuple path, byte[] layer/*=None*/) {
        Object prefix = node_subspace.unpack(node.getKey()).get(0);
        return new DirectorySubspace(path, Tuple.from(prefix).pack(), this, layer);
    }

    private Subspace _find(Transaction tr, Tuple path) {
        Subspace n = root_node;
        for(Object name : path) {
            n = _node_with_prefix(tr.get(n.get(SUBDIRS).get(name).getKey()).get());
            if(n == null) {
                return null;
            }
        }
        return n;
    }

    private void _remove_from_parent(Transaction tr, Tuple path) {
        Subspace parent = _find(tr, removeLast(path));
        tr.clear(parent.get(SUBDIRS).get(path.get(path.size() - 1)).getKey());
    }

    private void _remove_recursive(Transaction tr, Subspace node) {
        for(NameAndNode nn : _subdir_names_and_nodes(tr, node)) {
            _remove_recursive(tr, nn.node);
        }
        tr.clear(Range.startsWith(_contents_of_node(node, null, null).getKey()));
        tr.clear(node.range());
    }

    private boolean _is_prefix_free(Transaction tr, byte[] prefix) {
        // Returns true if the given prefix does not "intersect" any currently
        // allocated prefix (including the root node). This means that it neither
        // contains any other prefix nor is contained by any other prefix.
        if(prefix == null || prefix.length == 0) {
            return false;
        }
        if(_node_containing_key(tr, prefix) != null) {
            return false;
        }
        Iterator<KeyValue> it = tr.getRange(node_subspace.pack(prefix), node_subspace.pack(strinc(prefix)), 1).iterator();
        return !it.hasNext();
    }

    private Iterable<NameAndNode> _subdir_names_and_nodes(Transaction tr, Subspace node) {
        final Subspace sd = node.get(SUBDIRS);
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
                        Object name = sd.unpack(kv.getKey()).get(0);
                        Subspace node = _node_with_prefix(kv.getValue());
                        return new NameAndNode(Tuple.from(name).pack(), node);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }


    //
    // Helpers
    //

    private static class NameAndNode {
        public final byte[] name;
        public final Subspace node;

        public NameAndNode(byte[] name, Subspace node) {
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
        //@fdb.transactional
        public byte[] allocate(Transaction tr) {
            long start = 0, count = 0;

            Iterator<KeyValue> it = tr.snapshot().getRange(counters.range(), 1, true).iterator();
            if(it.hasNext()) {
                KeyValue kv = it.next();
                start = counters.unpack(kv.getKey()).getLong(0);
                count = unpackLittleEndian(kv.getValue());
            }

            int window = _window_size(start);
            if((count + 1) * 2 >= window) {
                // Advance the window
                byte[] begin = counters.getKey();
                byte[] end = join(counters.get(start).getKey(), new byte[]{ 0x00 });
                tr.clear(begin, end);
                start += window;
                tr.clear(recent.getKey(), recent.get(start).getKey());
                window = _window_size(start);
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

        private static int _window_size(long start) {
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

    private static long unpackLittleEndian(byte[] bytes) {
        assert bytes.length == 8;
        int value = 0;
        for(int i = 0; i < 8; ++i) {
            value += (bytes[i] << (i * 8));
        }
        return value;
    }

    private static Tuple removeLast(Tuple t) {
        if(t.size() > 0) {
            return Tuple.fromItems(t.getItems().subList(0, t.size() - 1));
        }
        return t;
    }
}