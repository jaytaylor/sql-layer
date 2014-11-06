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
package com.foundationdb.qp.storeadapter.indexcursor;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter.KeyReader;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter.KeyWriter;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter.SortKey;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;

public class KeyReaderWriterTest {

    private ByteArrayOutputStream os;
    private ByteArrayInputStream is;
    private SortKey startKey;
    private Key testKey;
    private KeyWriter writer;
    
    @Before
    public void createFileBuffers() {
        os = new ByteArrayOutputStream();
        startKey = new SortKey();
        testKey = new Key ((Persistit)null);
        writer = new KeyWriter(os);
    }
    @Test
    public void cycleSimple() throws IOException {
        testKey.append(1);
        startKey.sortKeys.add(new KeyState(testKey));
        startKey.rowValue.put(1);
        writer.writeEntry(startKey);
        verifyInput();
    }
    
    @Test
    public void cycleString() throws IOException {
        testKey.append("abcd");
        startKey.sortKeys.add(new KeyState(testKey));
        startKey.rowValue.put("abcd");
        writer.writeEntry(startKey);
        verifyInput();
    }

    @Test
    public void cycleNIntegers() throws IOException {
        for (int i = 0; i < 400; i++) {
            testKey.append(i);
            startKey.rowValue.put(i);
        }
        startKey.sortKeys.add(new KeyState(testKey));
        writer.writeEntry(startKey);
        verifyInput();
    }
    
    @Test
    public void cycle2Keys() throws IOException {
        
        testKey.append(1);
        startKey.sortKeys.add(new KeyState(testKey));
        startKey.rowValue.put(1);
        writer.writeEntry(startKey);
        writer.writeEntry(startKey);
        
        is = new ByteArrayInputStream (os.toByteArray());
        KeyReader reader = new KeyReader (is);

        SortKey endKey = reader.readNext();
        assertTrue (startKey.rowValue.get().equals(endKey.rowValue.get()));
        assertTrue (startKey.sortKeys.get(0).compareTo(endKey.sortKeys.get(0)) == 0);
        endKey = reader.readNext();
        assertTrue (startKey.rowValue.get().equals(endKey.rowValue.get()));
        assertTrue (startKey.sortKeys.get(0).compareTo(endKey.sortKeys.get(0)) == 0);
        endKey = reader.readNext();
        assertNull (endKey);
    }
    
    @Test
    public void cycleNKeys() throws IOException{
        List<SortKey> keys = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            SortKey newKey = new SortKey();
            newKey.rowValue.put(i);
            testKey.clear();
            testKey.append(i);
            newKey.sortKeys.add(new KeyState (testKey));
            keys.add(newKey);
        }
        verifyNKeys (keys);
    }
    
    @Test
    public void cycleNStrings() throws IOException {
        List<SortKey> keys = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            SortKey newKey = new SortKey();
            String value = characters(5+random.nextInt(1000));
            newKey.rowValue.put(value);
            testKey.clear();
            testKey.append(value);
            newKey.sortKeys.add(new KeyState (testKey));
            keys.add(newKey);
        }
        verifyNKeys (keys);
    }
    
    @Test
    public void cycleNMultiKeys () throws IOException {
        List<SortKey> keys = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            SortKey newKey = new SortKey();
            newKey.rowValue.setStreamMode(true);
            newKey.rowValue.put(random.nextInt());
            newKey.rowValue.putNull();
            newKey.rowValue.put(characters(3+random.nextInt(25)));
            newKey.rowValue.put(characters(3+random.nextInt(25)));
            testKey.clear();
            testKey.append(i);
            newKey.sortKeys.add(new KeyState (testKey));
            keys.add(newKey);
        }
        verifyNKeys(keys);
    }
    
    private void verifyInput() throws IOException {
        is = new ByteArrayInputStream (os.toByteArray());
        KeyReader reader = new KeyReader (is);
        SortKey endKey = reader.readNext();
        assertTrue (startKey.rowValue.get().equals(endKey.rowValue.get()));
        assertTrue (startKey.sortKeys.get(0).compareTo(endKey.sortKeys.get(0)) == 0);
        
    }
    
    private void verifyNKeys(List<SortKey> keys) throws IOException  {
        for (SortKey key : keys) {
            writer.writeEntry(key);
        }
        is = new ByteArrayInputStream (os.toByteArray());
        KeyReader reader = new KeyReader (is);

        SortKey endKey;
        for (SortKey startKey : keys) {
            endKey = reader.readNext();
            endKey.rowValue.setStreamMode(true);
            startKey.rowValue.setStreamMode(true);
            assertTrue (startKey.rowValue.get().equals(endKey.rowValue.get()));
            assertTrue (startKey.sortKeys.get(0).compareTo(endKey.sortKeys.get(0)) == 0);
        }
    }

    static final String ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    final Random random = new Random(100);
    public String characters(final int length) {
        StringBuilder sb = new StringBuilder(length);
        for( int i = 0; i < length; i++ ) 
           sb.append(ALPHA.charAt(random.nextInt(ALPHA.length())));
        return sb.toString();
     }
}
