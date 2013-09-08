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
import com.foundationdb.FDB;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DirectoryIT
{
    private static List<List<Object>> unpack(List<byte[]> byteList) {
        List<List<Object>> outList = new ArrayList<>();
        for(byte[] bytes : byteList) {
            outList.add(Tuple.fromBytes(bytes).getItems());
        }
        return outList;
    }

    private static List<Object> oList(Object... objects) {
        return Arrays.asList(objects);
    }

    @Test
    public void test() {
        FDB fdb = FDB.selectAPIVersion(100);
        Database db = fdb.open();
        Transaction tr = db.createTransaction();
        tr.clear(new byte[]{ 0x00 }, new byte[]{ (byte)0xFF });


        DirectorySubspace evilDir = new Directory().create(tr, Tuple.from("evil"), null, new byte[]{ 0x14 });
        System.out.println("evil is in: " + evilDir);

        Directory directory = Directory.createWithContentSubspace(new Subspace(new byte[]{0x01}));

        // Make a new directory
        DirectorySubspace stuff = directory.create(tr, Tuple.from("stuff"), null, null);
        System.out.println("stuff is in: " + stuff);
        System.out.println("stuff[0] is: " + DirectorySubspace.tupleStr(Tuple.fromBytes(stuff.get(0).getKey())));
        //assert stuff.key() == "\x01\x14"

        // Open it again
        DirectorySubspace stuff2 = directory.open(tr, Tuple.from("stuff"), null);
        assertArrayEquals(stuff.getKey(), stuff2.getKey());

        // Make another directory
        DirectorySubspace items = directory.create_or_open(tr, Tuple.from("items"), null, null, true, true);
        System.out.println("items are in: " + items);
        //assert items.key() == "\x01\x15\x01"

        // List the root directory
        assertEquals(
            asList(oList("evil"), oList("items"), oList("stuff")),
            unpack(directory.list(tr, Tuple.from()))
        );

        // Move everything into an "app" directory
        directory.create(tr, Tuple.from("app"), null, null);
        directory.move(tr, Tuple.from("stuff"), Tuple.from("app", "stuff"));
        directory.move(tr, Tuple.from("items"), Tuple.from("app", "items"));

        // Make a directory in a hard-coded place
        DirectorySubspace special = directory.create_or_open(
            tr,
            Tuple.from("app", "special"),
            null,
            new byte[]{ 0x00 },
            true,
            true
        );
        assertArrayEquals(new byte[]{ 0x00 }, special.getKey());

        assertEquals(
            asList(oList("app"), oList("evil")),
            unpack(directory.list(tr, Tuple.from()))
        );
        assertEquals(
            asList(oList("items"), oList("special"), oList("stuff")),
            unpack(directory.list(tr, Tuple.from("app")))
        );

        DirectorySubspace dir2 = directory.open(tr, Tuple.from("app", "stuff"), null);
        assertArrayEquals(stuff.getKey(), dir2.getKey());

        // Destroy the stuff directory
        directory.remove(tr, Tuple.from("app", "stuff"));
        try {
            directory.open(tr, Tuple.from("app", "stuff"), null);
            fail("expected error");
        } catch(IllegalArgumentException e) {
            // Expected
        }

        assertEquals(
            asList(oList("items"), oList("special")),
            unpack(directory.list(tr, Tuple.from("app")))
        );

        // Test that items is still OK
        DirectorySubspace items2 = directory.create_or_open(tr, Tuple.from("app", "items"), null, null, true, true);
        assertArrayEquals(items.getKey(), items2.getKey());
    }
}
