package com.akiban.cserver.store;

import java.nio.*;
import java.nio.channels.*;
import java.io.*;


/**
 * @author percent
 *
 */

public class FixedLengthArray implements FieldArray {
    
	public FixedLengthArray(File file, int fieldSize) throws FileNotFoundException, IOException {
	    in = new FileInputStream(file);
        channel = in.getChannel();
        this.fieldSize = fieldSize;
        columnSize = channel.size();
        mapSize = 50*1048576;
        offset = 0;
        load();
	}
	
	@Override
    public int getNextFieldSize() throws IOException {
        return fieldSize;
    }
    
    @Override
    public boolean copyNextField(byte[] dest, int offset) throws IOException {
        /*
        System.out.println("fixed length array: column.position() =" 
                +column.position()+" fieldSize = "
                + fieldSize+ " capacitiy = "+column.capacity());
        */
        assert column.position() + fieldSize <= column.capacity();
        assert dest.length - offset >= fieldSize;
        column.get(dest, offset, fieldSize);

        //for(int m = 0; m < fieldSize; m++) {
        //    System.out.print((Integer.toHexString((int)dest[m] & 0x000000ff))+ ", ");
        //}
        //System.out.println();
        if(column.position() == column.capacity() && this.offset != columnSize) {
            load();
        }
        assert column.position() <= column.capacity();
        return (column.position() == column.capacity() && this.offset == columnSize ? false : true);
    }
    
    @Override
    public long getColumnSize() {
        return columnSize;
    }
        
    public void close() {
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void load() throws IOException {
        int size = mapSize;
        if(size > columnSize - offset) {
            size = (int)(columnSize - offset);
        }
        column = channel.map(FileChannel.MapMode.READ_ONLY, offset, size);
        assert size == column.capacity();
        offset += size;
        column.load();
    }
    private FileInputStream in;
    private FileChannel channel;
    private MappedByteBuffer column;
    private int fieldSize;
    private int mapSize;
    private long columnSize;
    private int offset;
}
