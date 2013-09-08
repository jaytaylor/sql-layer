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

import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.foundationdb.util.layers.Directory.NoSuchDirectoryException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DirectoryIT extends LayerITBase
{
    private Transaction tr;

    @Before
    public final void cleanSlate() {
        tr = holder.getDatabase().createTransaction();
        tr.clear(new byte[]{ 0x00 }, new byte[]{ (byte)0xFF });
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

    private static List<Object> oList(Object... objects) {
        return Arrays.asList(objects);
    }
}
