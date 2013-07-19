/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
package com.akiban.qp.persistitadapter.indexcursor;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.akiban.qp.persistitadapter.indexcursor.MergeJoinSorter.KeyReader;
import com.akiban.qp.persistitadapter.indexcursor.MergeJoinSorter.KeyWriter;
import com.persistit.Key;
import com.persistit.Persistit;

public class TestKeyReaderWriter {

    private ByteArrayOutputStream os;
    private ByteArrayInputStream is;
    private Key startKey;
    private KeyWriter writer;
    
    @Before
    public void createFileBuffers() {
        os = new ByteArrayOutputStream();
        startKey = new Key ((Persistit)null);
        startKey.clear();
        writer = new KeyWriter(os);
    }
    @Test
    public void cycleSimple() throws IOException {
        startKey.append(1);
        writer.writeEntry(startKey);
        verifyInput();
    }
    
    @Test
    public void cycleString() throws IOException {
        startKey.append("abcd");
        writer.writeEntry(startKey);
        verifyInput();
    }

    @Test
    public void cycleNIntegers() throws IOException {
        for (int i = 0; i < 400; i++) {
            startKey.append(i);
        }
        writer.writeEntry(startKey);
        verifyInput();
    }
    
    @Test
    public void cycle2Keys() throws IOException {
        startKey.append(1);
        writer.writeEntry(startKey);
        writer.writeEntry(startKey);
        
        is = new ByteArrayInputStream (os.toByteArray());
        KeyReader reader = new KeyReader (is);

        Key endKey = reader.readNext();
        assertTrue (startKey.compareTo(endKey) == 0);
        endKey = reader.readNext();
        assertTrue (startKey.compareTo(endKey) == 0);
    }
    
    @Test
    public void cycleNKeys() throws IOException{
        List<Key> keys = new ArrayList<Key>(100);
        for (int i = 0; i < 100; i++) {
            Key newKey = new Key ((Persistit)null);
            newKey.append(i);
            keys.add(newKey);
        }
        verifyNKeys (keys);
    }
    
    @Test
    public void cycleNStrings() throws IOException {
        List<Key> keys = new ArrayList<Key>(100);
        for (int i = 0; i < 100; i++) {
            Key newKey = new Key ((Persistit)null);
            newKey.append(characters(5+random.nextInt(1000)));
            keys.add(newKey);
        }
        verifyNKeys (keys);
    }
    
    @Test
    public void cycleNMultiKeys () throws IOException {
        List<Key> keys = new ArrayList<Key>(100);
        for (int i = 0; i < 100; i++) {
            Key newKey = new Key ((Persistit)null);
            newKey.append(random.nextInt());
            newKey.append(null);
            newKey.append(characters(3+random.nextInt(25)));
            newKey.append(characters(3+random.nextInt(25)));
            keys.add(newKey);
        }
        verifyNKeys(keys);
    }
    
    private void verifyInput() throws IOException {
        is = new ByteArrayInputStream (os.toByteArray());
        KeyReader reader = new KeyReader (is);
        Key endKey = reader.readNext();
        assertTrue (startKey.compareTo(endKey) == 0);
        
    }
    
    private void verifyNKeys(List<Key> keys) throws IOException  {
        for (Key key : keys) {
            writer.writeEntry(key);
        }
        is = new ByteArrayInputStream (os.toByteArray());
        KeyReader reader = new KeyReader (is);

        Key endKey;
        for (Key startKey : keys) {
            endKey = reader.readNext();
            assertTrue (startKey.compareTo(endKey) == 0);
        }
    }

    static final String ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static final Random random = new Random();
    private static String characters(final int length) {
        StringBuilder sb = new StringBuilder(length);
        for( int i = 0; i < length; i++ ) 
           sb.append(ALPHA.charAt(random.nextInt(ALPHA.length())));
        return sb.toString();
     }

}
