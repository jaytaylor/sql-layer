/**
 * 
 */
package com.akiban.cserver.store;

import java.io.*;
import java.util.*;

import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;

/**
 * @author percent
 *
 */
public class HKeyColumnArrayGenerator {
    
    public HKeyColumnArrayGenerator(String baseName) throws Exception {
        metaFile = new File(baseName+"-hkey.meta");
        dataFile = new File(baseName+"-hkey.data");
        keys = new ArrayList<KeyState>();
        FileOutputStream file = new FileOutputStream(metaFile);
        file.getChannel().truncate(0);
        file.flush();
        file.close();
        file = new FileOutputStream(dataFile);
        file.getChannel().truncate(0);
        file.flush();
        file.close();
    }

    public void append(KeyState prefix, int numKeys) throws IOException {
        FileOutputStream metaOut = new FileOutputStream(metaFile, true);
        FileOutputStream dataOut = new FileOutputStream(dataFile, true);

        Formatter f = new Formatter();
        
        for(int i = 0; i < numKeys; i++) {
        
            Key hkey = new Key((Persistit)null);
            hkey.clear();
            prefix.copyTo(hkey);
            hkey.append(i);
            KeyState hkeyState = new KeyState(hkey);
            
            f.setInt(hkeyState.getBytes().length);
            metaOut.write(f.serialize());
            metaOut.flush();
            
        
            dataOut.write(hkeyState.getBytes());
            dataOut.flush();
            keys.add(hkeyState);
        }
        
        
        metaOut.close();
        dataOut.close();
        

    }
    
    public ArrayList<KeyState> getKeys() {
        return keys;
    }
        
    private class Formatter extends IFormat {
        public Formatter() {
            intSet = false;
            value = 0;
        }
        public void setInt(int intval) {
            intSet = true;
            value = intval;
        }
        public byte[] serialize() {
            assert intSet == true;
            byte[] bytes = new byte[4];
            packInt(bytes, 0, value);
            return bytes;
        }
        public boolean intSet;
        public int value;
    }
    
	
	private File dataFile;
	private File metaFile;
	private ArrayList<KeyState> keys;
	
}