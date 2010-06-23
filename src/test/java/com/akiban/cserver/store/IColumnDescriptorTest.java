/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.akiban.cserver.store.FixedLengthDescriptor;
import com.akiban.cserver.store.HKeyDescriptor;
import com.akiban.cserver.store.IColumnDescriptor;

/**
 * @author percent
 *
 */
public class IColumnDescriptorTest {

    @Test
    public void testFixedCreate() throws Exception {
        
        File file = new File("1"+"2"+"3");
        file.createNewFile();
        
        assertEquals(1024, IColumnDescriptor.getFormatSize());
        
        IColumnDescriptor f = IColumnDescriptor.create("", "1", "2", "3", 73, 31, 33, 847);
        assertTrue(f instanceof FixedLengthDescriptor);
        FixedLengthDescriptor format = (FixedLengthDescriptor)f;
        
        assertEquals(73, format.getTableId());
        assertEquals(31, format.getOrdinal());
        assertEquals(33, format.getFieldSize());
        assertEquals(847, format.getFieldCount());
        assertEquals("1", format.getSchema());
        assertEquals("2", format.getTable());
        assertEquals("3", format.getColumn());        
        assertEquals(0x490000001fL, format.getId());
        
        byte[] meta = format.serialize();
        
        f = IColumnDescriptor.create("", meta);
        assertTrue(f instanceof FixedLengthDescriptor);
        format = (FixedLengthDescriptor)f;

        assertEquals(73, format.getTableId());
        assertEquals(31, format.getOrdinal());
        assertEquals(33, format.getFieldSize());
        assertEquals(847, format.getFieldCount());
        assertEquals("1", format.getSchema());
        assertEquals("2", format.getTable());
        assertEquals("3", format.getColumn());
        assertEquals(0x490000001fL, format.getId());
    }
    
    @Test
    public void testHkeyCreate() throws Exception {
            

        File metaFile = new File("1"+"2"+"-hkey.meta");
        metaFile.createNewFile();
        File dataFile = new File("1"+"2"+"-hkey.data");
        dataFile.createNewFile();
                
        IColumnDescriptor f = IColumnDescriptor.create("", "1", "2", 73, 31);
        assertTrue(f instanceof HKeyDescriptor);
        HKeyDescriptor hformat = (HKeyDescriptor)f;
        
        assertEquals(73, hformat.getTableId());
        assertEquals(31, hformat.getFieldCount());
        assertEquals("1", hformat.getSchema());
        assertEquals("2", hformat.getTable());
        
        byte[] hmeta = hformat.serialize();
        f = IColumnDescriptor.create("", hmeta);
        assertTrue(f instanceof HKeyDescriptor);
        hformat = (HKeyDescriptor)f;
        assertEquals(73, hformat.getTableId());
        assertEquals(31, hformat.getFieldCount());
        assertEquals("1", hformat.getSchema());
        assertEquals("2", hformat.getTable());
    }
}
