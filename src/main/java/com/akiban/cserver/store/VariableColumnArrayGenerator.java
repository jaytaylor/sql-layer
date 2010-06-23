/**
 * 
 */
package com.akiban.cserver.store;

import java.io.*;
import java.util.*;

/**
 * @author percent
 *
 */
public class VariableColumnArrayGenerator {

    public VariableColumnArrayGenerator(int seed, int numFields) {
        this.seed = seed;
        this.numFields = numFields;
        dataFile = null;
        metaFile = null;
        random = null;
    }

    public VariableColumnArrayGenerator(String baseName, int seed, int numFields) {
        metaFile = new File(baseName+".meta");
        dataFile = new File(baseName+".data");
        random = null;
        this.seed = seed;
        this.numFields = numFields;
    }

    /*
    public void writeEncodedColumn(ArrayList<byte[]> column) throws FileNotFoundException, IOException {
        assert file != null;
        FileOutputStream out = new FileOutputStream(file);   
        for(int i = 0; i < numFields; i++) {
            assert column.get(i).length == fieldSize;            
            out.write(column.get(i));
            out.flush();
        }
        out.close();
    }
    */
    
    public void generateFile() throws FileNotFoundException, IOException {
	    assert metaFile != null && dataFile != null;
        
	    FileOutputStream metaOut = new FileOutputStream(metaFile);
	    FileOutputStream dataOut = new FileOutputStream(dataFile);
	    
	    metaOut.getChannel().truncate(0);
	    metaOut.flush();
	    dataOut.getChannel().truncate(0);
	    dataOut.flush();
	    
		Random localRandom = new Random(seed);
		Formatter f = new Formatter();
		
        for(int i = 0; i < numFields; i++) {
            int meta = 0;
            while(meta == 0) {
                meta =localRandom.nextInt(MAX_FIELD_SIZE);
            }
            byte[] field = new byte[meta]; 
            localRandom.nextBytes(field);
                        
            f.setInt(meta);
            metaOut.write(f.serialize());
            metaOut.flush();

            dataOut.write(field);
            dataOut.flush();
		}
        metaOut.close();
        dataOut.close();
	}

	public ArrayList<byte[]> generateMemoryFile(int numFields) {
		
		if(random == null) {
			random = new Random(seed);
		}
		
		ArrayList<byte[]> fields = new ArrayList<byte[]>();
		for(int i = 0; i < numFields; i++) {
		      int meta = 0;
		      while(meta == 0) {
		          meta = random.nextInt(MAX_FIELD_SIZE); 
		      }
		      
		      byte[] field = new byte[meta];
		      random.nextBytes(field);
		      fields.add(field);
		}
		return fields;
	}
	
	File getFile() {
		return dataFile;
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
    
	
    private static int MAX_FIELD_SIZE = 65536;
	private File dataFile;
	private File metaFile;
	private Random random;
	private int seed;
    private int numFields;
}