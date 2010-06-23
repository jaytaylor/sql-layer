/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;
import java.io.File;
import org.junit.Test;

import com.akiban.cserver.store.FieldArray;
import com.akiban.cserver.store.FixedLengthArray;
import com.akiban.cserver.store.FixedLengthDescriptor;

/**
 * @author percent
 *
 */
public class FixedLengthDescriptorTest {

    @Test
    public void testColumnDes() throws Exception {
        ColumnArrayGenerator g = new ColumnArrayGenerator("123", 1337, 33, 847);
        g.generateFile();
        
        
        File file = new File("1"+"2"+"3");
        file.createNewFile();
        
        assertEquals(1024, FixedLengthDescriptor.getFormatSize());
        
        FixedLengthDescriptor format = new FixedLengthDescriptor("", "1", "2", "3", 73, 31, 33, 847);
        assertEquals(73, format.getTableId());
        assertEquals(31, format.getOrdinal());
        assertEquals(33, format.getFieldSize());
        assertEquals(847, format.getFieldCount());
        assertEquals("1", format.getSchema());
        assertEquals("2", format.getTable());
        assertEquals("3", format.getColumn());        
        assertEquals(0x490000001fL, format.getId());
        FieldArray fa = format.createFieldArray();
        assertTrue(fa instanceof FixedLengthArray);
        assertEquals(33*847, fa.getColumnSize());
        
        byte[] meta = format.serialize();
        FixedLengthDescriptor format1 = new FixedLengthDescriptor("", meta);
        
        assertEquals(73, format1.getTableId());
        assertEquals(31, format1.getOrdinal());
        assertEquals(33, format1.getFieldSize());
        assertEquals(847, format1.getFieldCount());
        assertEquals("1", format1.getSchema());
        assertEquals("2", format1.getTable());
        assertEquals("3", format1.getColumn());
        assertEquals(0x490000001fL, format1.getId());
        assertTrue(fa instanceof FixedLengthArray);
        assertEquals(33*847, fa.getColumnSize());
    }
}
