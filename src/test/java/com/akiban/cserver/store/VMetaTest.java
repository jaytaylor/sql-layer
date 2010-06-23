/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.junit.Test;

import com.akiban.cserver.store.IColumnDescriptor;
import com.akiban.cserver.store.VMeta;
import com.akiban.cserver.store.VariableColumnArrayGenerator;

/**
 * @author percent
 *
 */
public class VMetaTest {

    @Test
    public void testMetaMetaFormat() {
        VMeta.MetaMetaFormat format = new VMeta.MetaMetaFormat(1337, 7331);
        assertEquals(1337, format.getHKeys());
        assertEquals(7331, format.getColumns());
        

        byte[] meta = format.serialize();
        VMeta.MetaMetaFormat format1 = new VMeta.MetaMetaFormat(meta);
        assertEquals(7331, format1.getColumns());
        assertEquals(1337, format1.getHKeys());
    }
    
    @Test
    public void testVMeta() throws Exception, IOException {
        
        ColumnArrayGenerator g = new ColumnArrayGenerator("123", 1337, 33, 847);
        g.generateFile();
        VariableColumnArrayGenerator vcag = new VariableColumnArrayGenerator("12-hkey", 73, 31);
        vcag.generateFile();           
        
        ArrayList<IColumnDescriptor> hkeys = new ArrayList<IColumnDescriptor>();
        ArrayList<IColumnDescriptor> columns = new ArrayList<IColumnDescriptor>();
        
        hkeys.add(IColumnDescriptor.create("", "1", "2", 73, 31));
        hkeys.add(IColumnDescriptor.create("", "1", "2", 73, 31));
        hkeys.add(IColumnDescriptor.create("", "1", "2", 73, 31));
        hkeys.add(IColumnDescriptor.create("", "1", "2", 73, 31));
        hkeys.add(IColumnDescriptor.create("", "1", "2", 73, 31));
        
        columns.add(IColumnDescriptor.create("", "1","2","3", 0x1337, 0xa, -1, 0x7331));
        columns.add(IColumnDescriptor.create("", "1","2","3", 0x1337, 0xb, -1, 0x7331));

        columns.add(IColumnDescriptor.create("", "1","2","3", 0x833, 0xc, -1, 0x123));
        columns.add(IColumnDescriptor.create("", "1","2","3", 0x833, 0xd, -1, 0x123));
        columns.add(IColumnDescriptor.create("", "1","2","3", 0x833, 0xe, -1, 0x123));

        File metaFile = new File("vmeta-test-file");
        try {
            VMeta vmeta = new VMeta(hkeys, columns);
            vmeta.write(metaFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            assertTrue(false);
        } catch (IOException e) {
            e.printStackTrace();
            assertTrue(false);
        }

        try {
            VMeta vmeta1 = new VMeta(new File("vmeta-test-file"));
            Collection<IColumnDescriptor> diskMeta = vmeta1.getColumns();
            assertEquals(columns.size(),diskMeta.size());
            Iterator<IColumnDescriptor> i = columns.iterator();
            while(i.hasNext()) {
                IColumnDescriptor cdes = i.next();
                int found = 0;
                Iterator<IColumnDescriptor> j = diskMeta.iterator();
                while(j.hasNext()) {
                    IColumnDescriptor cdesDisk = j.next();
                    
                    if(cdes.getTableId() == cdesDisk.getTableId()
                       && cdes.getOrdinal() == cdesDisk.getOrdinal()) {
                        found++;
                        assertEquals(cdes.getFieldCount(), cdesDisk.getFieldCount());
                    }
                }
                assertEquals(1, found);
            }
            

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            assertTrue(false);
        } catch (IOException e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
}
