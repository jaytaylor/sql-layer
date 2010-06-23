/**
 * 
 */
package com.akiban.cserver.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;


/**
 * @author percent
 *
 */
public class VariableLengthArray implements FieldArray {

    public VariableLengthArray(File meta, File data)
            throws FileNotFoundException, IOException {
        mapSize = 40*1048576;
        
        metaIn = new FileInputStream(meta);
        metaChan = metaIn.getChannel();
        metaColumnSize = metaChan.size();
        assert metaColumnSize % 4 == 0;
        metaOffset = 0;
        this.meta = null; 
        loadMeta();
        
        dataIn = new FileInputStream(data);
        dataChan = dataIn.getChannel();
        dataColumnSize = dataChan.size();
        dataOffset = 0;

        loadData();
    }

    @Override
    public int getNextFieldSize() {
        return meta[metaIndex];
    }
    
    @Override
    public boolean copyNextField(byte[] dest, int offset) throws IOException {
        assert dataColumn.position() + meta[metaIndex] <= dataColumn.capacity();
        assert dest.length - offset >= meta[metaIndex];
        dataColumn.get(dest, offset, meta[metaIndex]);
        metaIndex++;
        
        if(metaIndex == meta.length && metaOffset != metaColumnSize) {
            loadMeta();
        } 
        assert metaIndex <= meta.length;
        
        if(dataColumn.position() == dataColumn.capacity() && dataOffset != dataColumnSize) {
            loadData();
        }
        assert dataColumn.position() <= dataColumn.capacity();
        
        return (dataColumn.position() == dataColumn.capacity() 
                && dataOffset == dataColumnSize ? false : true);
    }
    
    private void loadMeta() throws IOException {
        int size = mapSize;
        if(size > (int)metaColumnSize - metaOffset) {
            size = (int)metaColumnSize - metaOffset;
        }
        assert size >= 0;
        if(size <= 0) {
            System.out.println("metaOffset  = "+ metaOffset +" metaColumnSize "+metaColumnSize +", mapSize"+mapSize);
        }
        assert size > 0;
        
        metaColumn =  metaChan.map(FileChannel.MapMode.READ_ONLY, metaOffset, size);
        metaColumn.load();
        metaOffset += size;
        assert size == metaColumn.capacity();
        assert size % 4 == 0;

        meta = new int[size/4];
        for(int i = 0; i < size/4; i++) {
            meta[i] = metaColumn.getInt();
        }
        metaIndex = 0;
    }
    
    private void loadData() throws IOException {
        int size = mapSize;
        if(size > (int)dataColumnSize - dataOffset) {
            size = (int)dataColumnSize - dataOffset;
        }
        dataColumn =  dataChan.map(FileChannel.MapMode.READ_ONLY, dataOffset, size);
        assert size == dataColumn.capacity();
        dataOffset += size;
        dataColumn.load();
    }
    
    public long getColumnSize() {
        return dataColumnSize;
    }
        
    public void close() {
        try {
            metaIn.close();
            dataIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private FileInputStream metaIn;
    private FileInputStream dataIn;
    private FileChannel metaChan;
    private FileChannel dataChan;
    private MappedByteBuffer metaColumn;
    private MappedByteBuffer dataColumn;
    private long metaColumnSize;
    private long dataColumnSize;
    private int mapSize;
    private int dataOffset;
    private int metaOffset;
    private int[] meta;
    private int metaIndex;
}
