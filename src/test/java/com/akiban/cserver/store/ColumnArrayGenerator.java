/**
 * 
 */
package com.akiban.cserver.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/**
 * @author percent
 *
 */
public class ColumnArrayGenerator {
    public ColumnArrayGenerator(String fileName, int seed, int fieldSize, int numFields) throws Exception {
        this.seed = seed;
        this.fieldSize = fieldSize;
        this.numFields = numFields;
        file = new File(fileName);
        FileOutputStream out = new FileOutputStream(file);
        out.getChannel().truncate(0);
        out.flush();
        out.close();
        random = null;
    }
    
    public ColumnArrayGenerator(int seed, int fieldSize, int numFields) {
        this.seed = seed;
        this.fieldSize = fieldSize;
        this.numFields = numFields;
        file = null;
        random = null;
    }
	
    public void writeEncodedColumn(ArrayList<byte[]> column) throws FileNotFoundException, IOException {
        assert file != null;
        FileOutputStream out = new FileOutputStream(file);
        //System.out.println("columnArrayGenerator, filename, size"+file+","+fieldSize);
        for(int i = 0; i < numFields; i++) {
            assert column.get(i).length == fieldSize;            
            out.write(column.get(i));
            out.flush();
        }
        out.close();
    }
    
    public void generateFile() throws FileNotFoundException, IOException {
	    assert file != null;
        FileOutputStream out = new FileOutputStream(file);
		Random localRandom = new Random(seed);
		byte [] buffer = new byte[fieldSize];
		
        for(int i = 0; i < numFields; i++) {
            localRandom.nextBytes(buffer);
            out.write(buffer);
            out.flush();
		}
        out.close();
	}

	public ArrayList<byte[]> generateMemoryFile(int numFields) {
		
		if(random == null) {
			random = new Random(seed);
		}
		
		ArrayList<byte[]> fields = new ArrayList<byte[]>();
		byte [] field = new byte[fieldSize];
		for(int i = 0; i < numFields; i++) {
			random.nextBytes(field);
			fields.add(field);
			field = new byte[fieldSize];
		}
		return fields;
	}
	
	File getFile() {
		return file;
	}
	
	public static void main(String [] args) throws Exception {
		// Quick test: hexdump the 2 files to verify that they are equivalent.		
		ColumnArrayGenerator one = new ColumnArrayGenerator("file1", 31337, 128, 1337);
		ColumnArrayGenerator two = new ColumnArrayGenerator("file2", 31337, 128, 1337);
		try { 
			one.generateFile();
			two.generateFile();
		} catch(Exception e) {
			System.out.println("EXception " +e.toString());
		}
	}
	
	private File file;
	private Random random;
	private int seed;
	private int fieldSize;
	private int numFields;
}