package com.akiban.vstore;

import java.nio.*;
import java.nio.channels.*;
import java.io.*;

/**
 * @author percent
 *
 */
public class ColumnArray {
	public ColumnArray(File file) throws FileNotFoundException, IOException {
		assert file != null;
		FileInputStream in = new FileInputStream(file);
		channel = in.getChannel();
		size = channel.size();
		cursor = 0;
	}
	
	public long getSize() {
		return size;
	}
	
	public void reset() {
		cursor = 0;
	}
	
	public void close() {
	    try {
            channel.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}
	
	public ByteBuffer map(long size) throws IOException {
	    //System.out.println("channel "+ channel + ", cusore = "+ cursor + ", size = "+ size);
		MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, cursor, size);
		buf.load();
		cursor += size;
		return buf;		
	}
	
	private FileChannel channel;
	private long cursor;
	private long size;	
}
