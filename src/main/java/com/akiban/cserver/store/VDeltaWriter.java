/**
 * 
 */
package com.akiban.cserver.store;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.store.DeltaMonitor.DeltaCursor;
import com.persistit.KeyState;

import com.akiban.vstore.IColumnDescriptor;
import com.akiban.vstore.IFormat;
import com.akiban.vstore.VMeta;

/**
 * @author percent
 */
public class VDeltaWriter {

    public VDeltaWriter(final String path, VMeta vmeta, DeltaCursor dcursor, 
            HashSet<RowDef> tables) {
        
        dataPath = path + "/vstore/";
        meta = vmeta;
        cursor = dcursor;
        this.tables = tables; 
        
        File dataPathFile = new File(dataPath);

        if (!dataPathFile.exists()) {
            if (!dataPathFile.mkdir()) {
                throw new Error("cannot create VStore directory");
            }
        }

        fileNameColumnNameMap = new TreeMap<String, String>();
        columnInfo = new TreeMap<String, VWriterInfo>();
        hkeyInfo = new TreeMap<Integer, VWriterInfo>();
    }

    public void write() throws Exception {
        //assert meta == null;
        
        if(meta == null) {
            //System.out.println("cursor.get = " + cursor.get());
            writeDelta();
        } else {
            rewrite();
        }
    }


    private void rewrite() throws Exception {
        VRewriteHelper helper = new VRewriteHelper(meta, cursor, tables);
        System.out.println("get total rows = "+ helper.getTotalRows());
        
        for(int rowCount = 0; rowCount < helper.getTotalRows(); rowCount++) {
            Delta d = helper.getNextRow();
            assert d != null;
            assert d.getType() == Delta.Type.Insert;
            
            // KeyState hkeyState = new KeyState(hkey);
            // String schemaName = rowDef.getSchemaName();
            // String tableName = rowDef.getTableName();
            String prefix = dataPath+d.getRowDef().getSchemaName()+d.getRowDef().getTableName()+".rewrite-inprogress.";   
            //System.out.println
            writeKey(prefix, d.getKey(), d.getRowDef());
            writeRow(prefix, d.getRowDef(), d.getRowData());
        }
        //helper.close();
        
        //if(true) 
//            return;
        boolean deleted = false;
        
        Iterator<RowDef> i = tables.iterator();
        while(i.hasNext()) {
            
            RowDef r = i.next();
            String rewrite_prefix = dataPath+r.getSchemaName()+r.getTableName()+".rewrite-inprogress.";
            String prefix = dataPath+r.getSchemaName()+r.getTableName();
            
            
            File oldHKeyMeta = new File(prefix + "-hkey.meta");
            if(oldHKeyMeta.exists()) {
                deleted = oldHKeyMeta.delete();
                assert deleted;
            }
            File newHKeyMeta = new File(rewrite_prefix + "-hkey.meta");
            boolean renamed = newHKeyMeta.renameTo(oldHKeyMeta);
            assert renamed;
            
            File oldHKeyData = new File(prefix + "-hkey.data");
            if(oldHKeyData.exists()) {
                deleted = oldHKeyData.delete();
                assert deleted;
            }
            File newHKeyData = new File(rewrite_prefix + "-hkey.data");
            renamed = newHKeyData.renameTo(oldHKeyData);
            assert renamed;

            
            for(int field = 0; field < r.getFieldCount(); field++) {
                FieldDef fieldDef = r.getFieldDef(field);
                String name = fieldDef.getName();
                
                File oldFile = new File(prefix+name);
                if(oldFile.exists()) {
                    deleted = oldFile.delete(); 
                    assert deleted;
                }
                File newFile = new File(rewrite_prefix+name);
                renamed = newFile.renameTo(oldFile);
                assert renamed;
            }
        }

        String metaFileName = dataPath + ".vmeta";
        File metaFile = new File(metaFileName);
        if(metaFile.exists()) {
            deleted = metaFile.delete();
            assert deleted;
        }
        createMeta();
    }
    
    private void writeDelta() throws Exception {
        while (cursor.get() != null) {
            Delta d = cursor.remove();
            assert d.getType() == Delta.Type.Insert;
            // KeyState hkeyState = new KeyState(hkey);
            // String schemaName = rowDef.getSchemaName();
            // String tableName = rowDef.getTableName();
            String prefix = dataPath + d.getRowDef().getSchemaName()
            + d.getRowDef().getTableName();
            writeKey(prefix, d.getKey(), d.getRowDef());
            writeRow(prefix, d.getRowDef(), d.getRowData());
        }        
        createMeta();        
    }
        
    private void createMeta() throws Exception {
        String prefix = dataPath;
        columnDescriptors = new ArrayList<IColumnDescriptor>();
        hkeyDescriptors = new ArrayList<IColumnDescriptor>();
        // System.out.println("columns ="+name);
        Iterator<VWriterInfo> i = columnInfo.values().iterator();
        while (i.hasNext()) {
            VWriterInfo info = i.next();
            IColumnDescriptor descrip = IColumnDescriptor.create(prefix, info
                    .getSchemaName(), info.getTableName(),
                    info.getColumnName(), info.getTableId(), info.getOrdinal(),
                    info.getSize(), info.getCount());

            //System.out.println("VDeltaWriter.createMeta: creating columnDes: "
            //+ descrip.getSchema() + descrip.getTable()
            //+ descrip.getColumn() + ", fieldCount = "
            //+ descrip.getFieldCount() + " id = " + descrip.getId());

            columnDescriptors.add(descrip);
        }

        Iterator<VWriterInfo> j = hkeyInfo.values().iterator();
        while (j.hasNext()) {
            VWriterInfo info = j.next();
            IColumnDescriptor hkeyDes = IColumnDescriptor.create(prefix, info
                    .getSchemaName(), info.getTableName(), info.getTableId(),
                    info.getCount());
            hkeyDescriptors.add(hkeyDes);
        }

        String metaFileName = dataPath + ".vmeta";
        File metaFile = new File(metaFileName);
        if(meta ==  null) {
            meta = new VMeta(hkeyDescriptors, columnDescriptors);
        } else {
            meta.append(hkeyDescriptors, columnDescriptors);
        }
        meta.write(metaFile);
    }

    private void writeKey(String prefix, KeyState key, RowDef rowDef)
            throws IOException {

        File hkeyMeta = new File(prefix + "-hkey.meta");
        File hkeyData = new File(prefix + "-hkey.data");
        System.out.println("writing key for tableID = " + rowDef.getRowDefId());
        FileOutputStream keyout = new FileOutputStream(hkeyMeta, true);
        byte[] metaSize = new byte[4];
        IFormat.packInt(metaSize, 0, key.getBytes().length);
        keyout.write(metaSize);
        keyout.flush();
        keyout.close();
        keyout = new FileOutputStream(hkeyData, true);
        keyout.write(key.getBytes());
        keyout.flush();
        keyout.close();

        if (hkeyInfo.get(rowDef.getRowDefId()) == null) {
            VWriterInfo info = new VWriterInfo("hkey", rowDef
                    .getTableName(), rowDef.getSchemaName(), rowDef
                    .getRowDefId(), -1);
            info.incrementCount();
            hkeyInfo.put(rowDef.getRowDefId(), info);
        } else {
            hkeyInfo.get(rowDef.getRowDefId()).incrementCount();
        }
    }

    private void writeRow(String prefix, RowDef rowDef, RowData rowData)
            throws Exception {

        for (int i = 0; i < rowDef.getFieldCount(); i++) {
            FieldDef field = rowDef.getFieldDef(i);

            String name = field.getName();
            System.out.println("prefix= "+prefix+", name = " + name);
            String fileName = prefix + name;
            File columnFile = new File(fileName);
            VWriterInfo info = columnInfo.get(fileName);
            if (!columnFile.exists() || info == null) {
                // XXX - this should not be managed here.
                columnFile.delete();
                boolean ret = columnFile.createNewFile();
                if (!ret) {
                    throw new Exception();
                }
                fileNameColumnNameMap.put(fileName, name);
                info = new VWriterInfo(name, rowDef.getTableName(),
                        rowDef.getSchemaName(), rowDef.getRowDefId(), i);
                columnInfo.put(fileName, info);
            }

            long locationAndSize = rowDef.fieldLocation(rowData, i);
            if (0 == locationAndSize) {
                // XXX - nulls are not supported.
                System.out.println("VDeltaWriter.writeRow: null field????  schema.table.name = "+rowDef.getSchemaName()+"."+rowDef.getTableName()+"."+name);
                //throw new Exception();
            }
            int offset = (int) locationAndSize;
            int size = (int) (locationAndSize >>> 32);
            byte[] bytes = rowData.getBytes();
            FileOutputStream fout = new FileOutputStream(columnFile, true);
            fout.write(bytes, offset, size);

            info.incrementCount();
            info.setSize(size);
            // columnInfo.put(name, info);
            fout.flush();
            fout.close();
        }
    }

    public VMeta getMeta() {
        return meta;
    }

    private String dataPath;
    private VMeta meta;
    private DeltaCursor cursor;
    
    private HashSet<RowDef> tables;
    private TreeMap<String, String> fileNameColumnNameMap;
    private TreeMap<String, VWriterInfo> columnInfo;
    private TreeMap<Integer, VWriterInfo> hkeyInfo;
    private ArrayList<IColumnDescriptor> columnDescriptors;
    private ArrayList<IColumnDescriptor> hkeyDescriptors;
}
