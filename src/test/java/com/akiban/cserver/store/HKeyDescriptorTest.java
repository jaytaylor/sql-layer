/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;
import org.junit.Test;

import com.akiban.cserver.store.FieldArray;
import com.akiban.cserver.store.HKeyDescriptor;
import com.akiban.cserver.store.VariableColumnArrayGenerator;
import com.akiban.cserver.store.VariableLengthArray;

/**
 * @author percent
 *
 */
public class HKeyDescriptorTest {

    @Test
    public void testHKeyDescriptor() throws Exception {
        
        VariableColumnArrayGenerator vcag = new VariableColumnArrayGenerator("12-hkey", 73, 31);
        vcag.generateFile();        
        
        assertEquals(1024, HKeyDescriptor.getFormatSize());
        
        HKeyDescriptor format = new HKeyDescriptor("", "1", "2", 73, 31);
        assertEquals(73, format.getTableId());
        assertEquals(31, format.getFieldCount());
        assertEquals("1", format.getSchema());
        assertEquals("2", format.getTable());
        FieldArray fa = format.createFieldArray();
        assertTrue(fa instanceof VariableLengthArray);
        
        byte[] meta = format.serialize();
        HKeyDescriptor format1 = new HKeyDescriptor("", meta);
        assertEquals(73, format1.getTableId());
        assertEquals(31, format1.getFieldCount());
        assertEquals("1", format1.getSchema());
        assertEquals("2", format1.getTable());
        fa = format.createFieldArray();
        assertTrue(fa instanceof VariableLengthArray);
    }
}
