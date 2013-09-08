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

import com.foundationdb.Database;
import com.foundationdb.FDBException;
import com.foundationdb.MutationType;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.sql.optimizer.plan.HashJoinNode;
import com.foundationdb.tuple.Tuple;
import com.foundationdb.util.layers.Directory.DirectoryAlreadyExistsException;
import com.foundationdb.util.layers.Directory.MismatchedLayerException;
import com.foundationdb.util.layers.Directory.NoSuchDirectoryException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DirectoryIT extends LayerITBase
{
    private static final Tuple PATH_A = Tuple.from("path_a");
    private static final Tuple PATH_B = Tuple.from("path_b");
    private static final byte[] LAYER_A = { (byte)'l', (byte)'a' };
    private static final byte[] LAYER_B = { (byte)'l', (byte)'b' };
    private static final byte[] PREFIX_A = { (byte)'p', (byte)'a' };

    private static List<Object> oList(Object... objects) {
        return Arrays.asList(objects);
    }


    private Transaction tr;
    private Directory dir;

    @Before
    public final void cleanSlate() {
        tr = holder.getDatabase().createTransaction();
        tr.clear(new byte[]{ 0x00 }, new byte[]{ (byte)0xFF });
        dir = new Directory();
    }

    @Test(expected=IllegalArgumentException.class)
    public void openNull() {
        dir.open(tr, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void openEmptyTuple() {
        dir.open(tr, Tuple.from());
    }

    @Test(expected=NoSuchDirectoryException.class)
    public void openNoSuch() {
        dir.open(tr, PATH_A);
    }

    @Test
    public void createAndOpen() {
        DirectorySubspace fromCreate = dir.create(tr, PATH_A);
        DirectorySubspace fromOpen = dir.open(tr, PATH_A);
        assertEquals(fromCreate, fromOpen);
    }

    @Test(expected=DirectoryAlreadyExistsException.class)
    public void createTwice() {
        dir.create(tr, PATH_A);
        dir.create(tr, PATH_A);
    }

    @Test
    public void createOrOpenTwice() {
        DirectorySubspace first = dir.createOrOpen(tr, PATH_A);
        DirectorySubspace second = dir.createOrOpen(tr, PATH_A);
        assertEquals(first, second);
    }

    @Test
    public void createWithNumericName() {
        Tuple path = Tuple.from(42);
        DirectorySubspace fromCreate = dir.create(tr, path);
        DirectorySubspace fromOpen = dir.open(tr, path);
        assertEquals(fromCreate, fromOpen);
    }

    @Test
    public void createWithByteName() {
        Tuple path = Tuple.from().add(new byte[] { 0x01, 0x02, 0x03});
        DirectorySubspace fromCreate = dir.create(tr, path);
        DirectorySubspace fromOpen = dir.open(tr, path);
        assertEquals(fromCreate, fromOpen);
    }

    @Test
    public void createWithMixedPath() {
        Tuple path = Tuple.from("a", 66, new byte[]{0x67});
        DirectorySubspace fromCreate = dir.create(tr, path);
        DirectorySubspace fromOpen = dir.open(tr, path);
        assertEquals(fromCreate, fromOpen);
    }

    @Test
    public void createWithLayer() {
        DirectorySubspace dirSub = dir.create(tr, PATH_A, LAYER_A);
        assertArrayEquals(LAYER_A, dirSub.getLayer());
    }

    @Test
    public void createWithLayerAndPrefix() {
        DirectorySubspace dirSub = dir.create(tr, PATH_A, LAYER_A, PREFIX_A);
        assertArrayEquals(LAYER_A, dirSub.getLayer());
        assertArrayEquals(PREFIX_A, dirSub.getKey());
        dirSub = dir.open(tr, PATH_A, LAYER_A);
        assertArrayEquals(LAYER_A, dirSub.getLayer());
        assertArrayEquals(PREFIX_A, dirSub.getKey());
    }

    @Test(expected=MismatchedLayerException.class)
    public void openWithWrongLayer() {
        dir.create(tr, PATH_A, LAYER_A);
        dir.open(tr, PATH_A, LAYER_B);
    }

    @Test
    public void createMoveOpen() {
        DirectorySubspace fromCreate = dir.create(tr, PATH_A);
        dir.move(tr, PATH_A, PATH_B);
        DirectorySubspace fromOpen = dir.open(tr, PATH_B);
        assertArrayEquals(fromCreate.getKey(), fromOpen.getKey());
        try {
            dir.open(tr, PATH_A);
            fail("expected NoSuchDirectoryException");
        } catch(NoSuchDirectoryException e) {
            // Expected
        }
    }

    @Test(expected=NoSuchDirectoryException.class)
    public void moveNoSuch() {
        dir.move(tr, PATH_A, PATH_B);
    }

    @Test(expected=DirectoryAlreadyExistsException.class)
    public void moveToSelf() {
        dir.create(tr, PATH_A);
        dir.move(tr, PATH_A, PATH_A);
    }

    @Test
    public void createRemove() {
        dir.create(tr, PATH_A);
        dir.open(tr, PATH_A);
        dir.remove(tr, PATH_A);
        try {
            dir.open(tr, PATH_A);
            fail("expected NoSuchDirectoryException");
        } catch(NoSuchDirectoryException e) {
            // Expected
        }
    }

    @Test(expected=NoSuchDirectoryException.class)
    public void removeNoSuch() {
        dir.remove(tr, PATH_A);
    }

    @Test
    public void createAndList() {
        dir.create(tr, Tuple.from("a"));
        dir.create(tr, Tuple.from("a", "b"));
        dir.create(tr, Tuple.from("a", "b", "c"));
        dir.create(tr, Tuple.from("a", "b", "c", "d"));
        dir.create(tr, Tuple.from("a", "foo"));
        dir.create(tr, Tuple.from("a", "zap"));
        dir.create(tr, Tuple.from("b"));
        dir.create(tr, Tuple.from("b", "b"));
        dir.create(tr, Tuple.from("b", "b", "c"));

        assertEquals(oList("a", "b"), dir.list(tr));
        assertEquals(oList("b", "foo", "zap"), dir.list(tr, Tuple.from("a")));
        assertEquals(oList("c"), dir.list(tr, Tuple.from("a", "b")));
        assertEquals(oList("d"), dir.list(tr, Tuple.from("a", "b", "c")));
        assertEquals(oList("b"), dir.list(tr, Tuple.from("b")));
        assertEquals(oList("c"), dir.list(tr, Tuple.from("b", "b")));
    }

    @Test
    public void createDeep() {
        dir.create(tr, Tuple.from("a", "b", "c", "d", "e"));
        dir.open(tr, Tuple.from("a", "b", "c", "d", "e"));
        dir.open(tr, Tuple.from("a", "b", "c", "d"));
        dir.open(tr, Tuple.from("a", "b", "c"));
        dir.open(tr, Tuple.from("a", "b"));
        dir.open(tr, Tuple.from("a"));
    }

    // Complete-ish test, which is a port of the original dirtest2.py
    @Test
    public void dirTest2() {
        DirectorySubspace evilDir = new Directory().create(tr, Tuple.from("evil"), null, new byte[]{ 0x14 });
        //System.out.println(evilDir.toString());

        Directory directory = Directory.createWithContentSubspace(new Subspace(new byte[]{0x01}));

        // Make a new directory
        DirectorySubspace stuff = directory.create(tr, Tuple.from("stuff"));
        //System.out.println("stuff is in: " + stuff);
        //System.out.println("stuff[0] is: " + DirectorySubspace.tupleStr(Tuple.fromBytes(stuff.get(0).getKey())));

        // Open it again
        DirectorySubspace stuff2 = directory.open(tr, Tuple.from("stuff"));
        assertArrayEquals(stuff.getKey(), stuff2.getKey());

        // Make another directory
        DirectorySubspace items = directory.createOrOpen(tr, Tuple.from("items"));
        //System.out.println("items are in: " + items);

        // List the root directory
        assertEquals(
            oList("evil", "items", "stuff"),
            directory.list(tr, Tuple.from())
        );

        // Move everything into an "app" directory
        directory.create(tr, Tuple.from("app"));
        directory.move(tr, Tuple.from("stuff"), Tuple.from("app", "stuff"));
        directory.move(tr, Tuple.from("items"), Tuple.from("app", "items"));

        // Make a directory in a hard-coded place
        DirectorySubspace special = directory.createOrOpen(tr, Tuple.from("app", "special"), null, new byte[]{ 0x00 });
        assertArrayEquals(new byte[]{ 0x00 }, special.getKey());

        assertEquals(
            oList("app", "evil"),
            directory.list(tr, Tuple.from())
        );
        assertEquals(
            oList("items", "special", "stuff"),
            directory.list(tr, Tuple.from("app"))
        );

        DirectorySubspace dir2 = directory.open(tr, Tuple.from("app", "stuff"), null);
        assertArrayEquals(stuff.getKey(), dir2.getKey());

        // Destroy the stuff directory
        directory.remove(tr, Tuple.from("app", "stuff"));
        try {
            directory.open(tr, Tuple.from("app", "stuff"));
            fail("expected NoSuchDirectoryException");
        } catch(NoSuchDirectoryException e) {
            // Expected
        }

        assertEquals(
            oList("items", "special"),
            directory.list(tr, Tuple.from("app"))
        );

        // Test that items is still OK
        DirectorySubspace items2 = directory.createOrOpen(tr, Tuple.from("app", "items"));
        assertArrayEquals(items.getKey(), items2.getKey());
    }


    @Ignore // Run manually
    @Test
    public void manyConcurrent() throws InterruptedException {
        final int THREAD_COUNT = 100;
        CyclicBarrier barrier = new CyclicBarrier(THREAD_COUNT);
        DirCreateThread threads[] = new DirCreateThread[THREAD_COUNT];
        for(int i = 0; i < THREAD_COUNT; ++i) {
            threads[i] = new DirCreateThread(holder.getDatabase(), barrier);
            threads[i].start();
        }

        int totalTotal = 0;
        for(DirCreateThread t : threads) {
            t.join();
            totalTotal += t.totalRetries;
        }
        System.out.println("totalRetries: " + totalTotal);

        int minPrefix = Integer.MAX_VALUE;
        int maxPrefix = Integer.MIN_VALUE;
        int avgPrefix = 0;
        Set<ByteBuffer> prefixes = new HashSet<>();
        for(DirCreateThread t : threads) {
            for(byte[] prefix : t.prefixList) {
                assertEquals("prefix unique", true, prefixes.add(ByteBuffer.wrap(prefix)));
                minPrefix = Math.min(minPrefix, prefix.length);
                maxPrefix = Math.max(minPrefix, prefix.length);
                avgPrefix += prefix.length;
            }
        }
        System.out.println("min prefix: " + minPrefix + ", max prefix: " + maxPrefix + ", avg prefix: " + (avgPrefix*1.0/prefixes.size()));

        assertEquals("total prefixes", THREAD_COUNT * DirCreateThread.COUNT, prefixes.size());
    }

    private static class DirCreateThread extends Thread {
        private static final int COUNT = 500;
        private final Database db;
        private final Directory dir;
        private final List<byte[]> prefixList;
        private final CyclicBarrier startBarrier;
        private int totalRetries;

        private DirCreateThread(Database db, CyclicBarrier startBarrier) {
            this.db = db;
            this.dir = new Directory();
            this.prefixList = new ArrayList<>(COUNT);
            this.startBarrier = startBarrier;
        }

        @Override
        public void run() {
            try {
                startBarrier.await();
            } catch(Exception e) {
                System.err.println("Thread failed: " + e);
                return;
            }

            Random rnd = new Random();
            int totalCounter = 0;
            int successCounter = 0;
            while(successCounter < COUNT) {
                Transaction tr = db.createTransaction();
                byte[] prefix = null;
                try {
                    totalCounter += (1 + rnd.nextInt(100));
                    DirectorySubspace dirSub = dir.create(tr, Tuple.from(totalCounter));
                    tr.commit().get();
                    prefix = dirSub.getKey();
                } catch(DirectoryAlreadyExistsException | FDBException e) {
                    // Retry
                }
                if(prefix != null) {
                    ++successCounter;
                    prefixList.add(prefix);
                } else {
                    ++totalRetries;
                }
            }
        }
    }
}
