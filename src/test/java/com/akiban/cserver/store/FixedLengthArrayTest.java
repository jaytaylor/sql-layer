/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;

import org.junit.Test;

import com.akiban.cserver.store.FixedLengthArray;
import com.akiban.cserver.store.IColumnDescriptor;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * @author percent
 *
 */
public class FixedLengthArrayTest {
    public static long chunkSize = 524248;
    
	@Test
	public void testCorrectness() throws Exception {
		try {
			ColumnArrayGenerator [] c;
			
			int [] fieldSize = { 16384, 32768, 65536};
			// XXX this test fail will fail if the numFields is not a multiple of 5
			int numFields = 805;

			c = new ColumnArrayGenerator[fieldSize.length];
			for(int i = 0; i < fieldSize.length; i++) {
				c[i] = new ColumnArrayGenerator("ColumnTestFile"+i, 31337, fieldSize[i], numFields);
				c[i].generateFile();
			}

			FixedLengthArray[] arrays = new FixedLengthArray[fieldSize.length];   
		    IColumnDescriptor[] cdes = new IColumnDescriptor[fieldSize.length];
		    byte[][] buffers = new byte[fieldSize.length][];
			
			for(int i = 0; i < fieldSize.length; i++) {
                cdes[i] = IColumnDescriptor.create("", "Column", "Test", "File"+i, 0,0,fieldSize[i], numFields);
                arrays[i] = new FixedLengthArray(new File("ColumnTestFile"+i), cdes[i].getFieldSize());
                buffers[i] = new byte[fieldSize[i]*5];
			}
			
			for(int i = 0; i < numFields; i += 5) {
			    for(int j=0; j < 5; j++) {
			        for(int k=0; k < fieldSize.length; k++) {
			            boolean ret = arrays[k].copyNextField(buffers[k], j*fieldSize[k]);
			            if(i+5 == numFields && j+1 == 5) {
			                assertFalse(ret);
			            } else {
			                assertTrue(ret);
			            }
			            byte[] actual = new byte[fieldSize[k]];
			            System.arraycopy(buffers[k], j*fieldSize[k], actual, 0, fieldSize[k]);
			            
		                ArrayList<byte[]> expectedContents = c[k].generateMemoryFile(1);
		                assert expectedContents.size() == 1;
		                    
		                Iterator<byte[]> l = expectedContents.iterator();
		                byte[] expected = l.next();
		                //System.out.println("Field (actual followd by expected) ");
                        /*for(int m = 0; m < expected.length; m++) {
                            System.out.println((Integer.toHexString((int)buffers[k][m+j*fieldSize[k]] & 0x000000ff)));
		                    System.out.println((Integer.toHexString((int)actual[m] & 0x000000ff)));
		                    System.out.println((Integer.toHexString((int)expected[m] & 0x000000ff)));
		                }*/
		                assertArrayEquals(expected, actual);
			        }
			    }
			}
		} catch(Exception e) {
		    e.printStackTrace();
		    throw e;
		}
	}

	@Test
	public void testPerformance() throws Exception {

		try {
/*			ColumnArrayGenerator [] c;
			
			int [] fieldSize = { 16384, 32768, 65536};
			int numFields = 8192;
			int totalData = numFields*(16384 + 32768 + 65536);

			c = new ColumnArrayGenerator[fieldSize.length];
			for(int i = 0; i < fieldSize.length; i++) {
				c[i] = new ColumnArrayGenerator("ColumnTestFile"+i, 31337, fieldSize[i], numFields);
				c[i].generateFile();
			}

			ColumnMapper mapper = new ColumnMapper();
			ColumnDescriptor[] cdes = new ColumnDescriptor[fieldSize.length];
			//ColumnArray[] ca = new ColumnArray[fieldSize.length];
			
			for(int i = 0; i < fieldSize.length; i++) {
                cdes[i] = new ColumnDescriptor("", "Column", "Test", "File"+i, 0,0,fieldSize[i], numFields);
                //ca[i] = cdes[i].createColumnArray();
				mapper.add(cdes[i]);
			}
			
			boolean done = false;
			long start = System.nanoTime();
			long totalRead=0;
			while(!done) {
                ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
                ColumnMapper.MapInfo ret = mapper.mapChunk(buffers, chunkSize);
                if(!ret.more) {
                    done = true;
                }
                
                Iterator<ByteBuffer> j = buffers.iterator();
                while(j.hasNext()) {
                    ByteBuffer buffer = j.next();
                    totalRead += buffer.capacity();
                    for(int k = 0; k < buffer.capacity(); k++) {
                        buffer.get(k);
                    }
                }
			}
			
			long totalNano = System.nanoTime() - start;
			long totalSecs = totalNano/1000000000;
			float MB = ((float)totalData)/((float)1048576);
			float MBPerSec = MB/(float)totalSecs;
			System.out.println("Data in MB = "+MB);
			System.out.println("Response time in seconds = "+totalSecs);
			System.out.println("Throughput in MB/s = " + MBPerSec);
	*/				
		} catch(Exception e) {
			throw e;
		}
	}
}
