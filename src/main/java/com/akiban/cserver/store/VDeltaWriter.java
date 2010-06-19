/**
 * 
 */
package com.akiban.cserver.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.io.File;
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

    public VDeltaWriter(final String path, VMeta vmeta, DeltaCursor dcursor) {
        cursor = dcursor;
        meta = vmeta;
        dataPath = path + "/vstore/";
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
        assert meta == null;
        System.out.println("cursor.get = " + cursor.get());
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
        meta = new VMeta(hkeyDescriptors, columnDescriptors);
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
            System.out.println("name = " + name);
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
                throw new Exception();
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

    private DeltaCursor cursor;
    private VMeta meta;
    private String dataPath;

    private TreeMap<String, String> fileNameColumnNameMap;
    private TreeMap<String, VWriterInfo> columnInfo;
    private TreeMap<Integer, VWriterInfo> hkeyInfo;
    private ArrayList<IColumnDescriptor> columnDescriptors;
    private ArrayList<IColumnDescriptor> hkeyDescriptors;
}
