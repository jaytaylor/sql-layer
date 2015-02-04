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

package com.foundationdb.server.service.blob;

import com.foundationdb.server.test.it.ITBase;

import java.util.UUID;
import java.util.Random;

import org.junit.*;


public class LobServiceIT extends ITBase {
    private LobService ls;
    private String idA;
    private String idB;
    private final byte[] data = "foo".getBytes();
    
    @Test
    public void utilizeLobService() {
        // registration
        Assert.assertNotNull(ls);

        // blob creation
        String id = UUID.randomUUID().toString();
        ls.createNewLob(id);

        ls.appendBlob(id, data);
        Assert.assertEquals(ls.sizeBlob(id), new Long(data.length).longValue());
        
        // blob transfer to new address
        String newPath = UUID.randomUUID().toString();
        ls.moveLob(id, newPath);
        
        // data retrieval
        byte[] output = ls.readBlob(newPath, 0, data.length);
        Assert.assertArrayEquals(data, output);
        output = ls.readBlob(newPath);
        Assert.assertArrayEquals(data, output);
        
        ls.truncateBlob(newPath, 2L);
        Assert.assertEquals(ls.sizeBlob(newPath), 2L);

        ls.truncateBlob(newPath, 0L);
        Assert.assertEquals(ls.sizeBlob(newPath), 0L);
        ls.appendBlob(newPath, data);
        output = ls.readBlob(newPath);
        Assert.assertArrayEquals(data, output);        
        
        ls.deleteLob(newPath);
        Assert.assertFalse(ls.existsLob(newPath));
    }
    
    @Before
    public void setUp(){
        // registration
        this.ls = serviceManager().getServiceByClass(LobService.class);
        Assert.assertNotNull(ls);

        idA = UUID.randomUUID().toString();
        idB = UUID.randomUUID().toString();
        ls.createNewLob(idA);
        ls.createNewLob(idB);

        ls.linkTableBlob(idA, 1);

    }
    
    @Test
    public void blobGarbageCollector() {
        // run collector
        ls.runLobGarbageCollector();
        
        // check cleaning
        Assert.assertTrue(ls.existsLob(idA));
        Assert.assertFalse(ls.existsLob(idB));
    }
    
    @Test
    public void performanceReadingWriting() {
        int lengthInMb = 100;

        byte[] data = generateBytes(1000000*lengthInMb);
        byte[] output;
        long start, stop;
        start = System.currentTimeMillis();
        ls.writeBlob(idA, 0, data);
        stop = System.currentTimeMillis();
        System.out.println("Writing --> time: " + ((stop - start)) + "ms, speed: " + (1000*(new Float(lengthInMb)/(stop-start)))+ " MB/sec");

        start = System.currentTimeMillis();
        output = ls.readBlob(idA);
        stop = System.currentTimeMillis();
        System.out.println("Reading --> time: " + ((stop - start)) + "ms, speed: " + (1000*(new Float(lengthInMb)/(stop-start)))+ " MB/sec");
        Assert.assertArrayEquals(data, output);        
        
        start = System.currentTimeMillis();
        output = ls.readBlob(idA, 0L, data.length);
        stop = System.currentTimeMillis();
        System.out.println("Reading as section--> time: " + ((stop - start)) + "ms, speed: " + (1000*(new Float(lengthInMb)/(stop-start)))+ " MB/sec");
        Assert.assertArrayEquals(data, output);

        start = System.currentTimeMillis();
        output = ls.readBlob(idA);
        stop = System.currentTimeMillis();
        System.out.println("Reading --> time: " + ((stop - start)) + "ms, speed: " + (1000*(new Float(lengthInMb)/(stop-start)))+ " MB/sec");

        start = System.currentTimeMillis();
        output = ls.readBlob(idA, 0L, data.length);
        stop = System.currentTimeMillis();
        System.out.println("Reading as section--> time: " + ((stop - start)) + "ms, speed: " + (1000*(new Float(lengthInMb)/(stop-start)))+ " MB/sec");

        start = System.currentTimeMillis();
        ls.writeBlob(idA, 0, data);
        stop = System.currentTimeMillis();
        System.out.println("Writing --> time: " + ((stop - start)) + "ms, speed: " + (1000*(new Float(lengthInMb)/(stop-start)))+ " MB/sec");

        start = System.currentTimeMillis();
        ls.writeBlob(idA, 0, data);
        stop = System.currentTimeMillis();
        System.out.println("Writing --> time: " + ((stop - start)) + "ms, speed: " + (1000*(new Float(lengthInMb)/(stop-start)))+ " MB/sec");
    }

    private byte[] generateBytes(int length) {
        byte[] inp = new byte[length];
        Random random = new Random();
        random.nextBytes(inp);
        return inp;
    }
}
