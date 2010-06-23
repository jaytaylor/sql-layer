/**
 * 
 */
package com.akiban.cserver.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.CRC32;


/**
 * @author percent
 * 
 */
public class VMeta {

    public VMeta(ArrayList<IColumnDescriptor> hkeys,
            ArrayList<IColumnDescriptor> columns) throws FileNotFoundException,
            IOException {
        hkeyMap = new TreeMap<Integer, IColumnDescriptor>();

        Iterator<IColumnDescriptor> i = hkeys.iterator();
        while (i.hasNext()) {

            IColumnDescriptor c = i.next();
            // System.out.println("VMeta: Adding columnDes: "+c.getSchema()+c.getTable()+c.getColumn()+", fieldCount = "+c.getFieldCount()+" id = "+c.getId());
            hkeyMap.put(c.getTableId(), c);
        }

        columnMap = new TreeMap<Long, IColumnDescriptor>();
        i = columns.iterator();
        while (i.hasNext()) {
            IColumnDescriptor c = i.next();
            // System.out.println("VMeta: Adding columnDes: "+c.getSchema()+c.getTable()+c.getColumn()+", fieldCount = "+c.getFieldCount()+" id = "+c.getId());
            assert c instanceof FixedLengthDescriptor;
            long id = c.getId();
            columnMap.put(id, c);
        }
    }

    public VMeta(File metaFile) throws FileNotFoundException, IOException {
        byte[] mmPacked = new byte[MetaMetaFormat.getFormatSize()];
        FileInputStream in = new FileInputStream(metaFile);
        in.read(mmPacked);
        MetaMetaFormat mm = new MetaMetaFormat(mmPacked);
        long hkeys = mm.getHKeys();
        long columns = mm.getColumns();

        hkeyMap = new TreeMap<Integer, IColumnDescriptor>();
        columnMap = new TreeMap<Long, IColumnDescriptor>();

        // System.out.println("VMeta: starting to read te column file: hkeys ="+hkeys+"columns = "+columns);

        for (int i = 0; i < hkeys + columns; i++) {
            byte[] mPacked = new byte[IColumnDescriptor.getFormatSize()];
            in.read(mPacked);
            IColumnDescriptor cdes = IColumnDescriptor.create(metaFile
                    .getAbsoluteFile().getParent(), mPacked);
            // System.out.println("VMeta: reading columnDes: "+cdes.getSchema()+cdes.getTable()+cdes.getColumn()+", fieldCount = "+cdes.getFieldCount()+" id = "+cdes.getId());

            if (i < hkeys) {
                assert cdes instanceof HKeyDescriptor;
                hkeyMap.put(cdes.getTableId(), cdes);
            } else {
                assert cdes instanceof FixedLengthDescriptor;
                long id = cdes.getId();
                columnMap.put(id, cdes);
            }
        }
        in.close();
        assert columns == columnMap.size();
    }

    public void append(ArrayList<IColumnDescriptor> hkeys,
            ArrayList<IColumnDescriptor> columns) throws FileNotFoundException,
            IOException {
        assert hkeyMap != null;
        assert columnMap != null;
        Iterator<IColumnDescriptor> i = hkeys.iterator();
        while (i.hasNext()) {
            IColumnDescriptor c = i.next();
            // System.out.println("VMeta: Adding columnDes: "+c.getSchema()+c.getTable()+c.getColumn()+", fieldCount = "+c.getFieldCount()+" id = "+c.getId());
            hkeyMap.put(c.getTableId(), c);
        }

        i = columns.iterator();
        while (i.hasNext()) {
            IColumnDescriptor c = i.next();
            // System.out.println("VMeta: Adding columnDes: "+c.getSchema()+c.getTable()+c.getColumn()+", fieldCount = "+c.getFieldCount()+" id = "+c.getId());
            assert c instanceof FixedLengthDescriptor;
            long id = c.getId();
            columnMap.put(id, c);
        }
    }

    public Collection<IColumnDescriptor> getColumns() {
        return columnMap.values();
    }

    public IColumnDescriptor getHKey(int tableId) {
        // assert hkeyMap.containsKey(tableId);
        return hkeyMap.get(tableId);
    }

    public void write(File metaFile) throws FileNotFoundException, IOException {

        MetaMetaFormat mm = new MetaMetaFormat(hkeyMap.size(), columnMap.size());
        byte[] mmPacked = mm.serialize();
        FileOutputStream out = new FileOutputStream(metaFile);
        out.write(mmPacked);

        Iterator<IColumnDescriptor> i = hkeyMap.values().iterator();
        while (i.hasNext()) {
            IColumnDescriptor c = i.next();
            // System.out.println("VMeta: writing columnDes: "+c.getSchema()+c.getTable()+c.getColumn()+", fieldCount = "+c.getFieldCount()+" id = "+c.getId());
            byte[] mPacked = c.serialize();
            out.write(mPacked);
        }

        i = columnMap.values().iterator();
        while (i.hasNext()) {
            IColumnDescriptor c = i.next();
            // System.out.println("VMeta: writing columnDes: "+c.getSchema()+c.getTable()+c.getColumn()+", fieldCount = "+c.getFieldCount()+" id = "+c.getId());
            byte[] mPacked = c.serialize();
            out.write(mPacked);
        }
        out.flush();
        out.close();
    }

    public IColumnDescriptor lookup(int tableId, int ordinal) {
        long columnId = (long) tableId << 32 | ordinal;
        // System.out.println("VMeta: get tableId = "+tableId+"ordinal = "+ordinal+", column ID = "+columnId);
        /*
         * try { Thread.currentThread().sleep(100); } catch
         * (InterruptedException e) { // TODO Auto-generated catch block
         * e.printStackTrace(); }
         */
        return columnMap.get(new Long(columnId));
    }

    public static class MetaMetaFormat extends IFormat {

        public static int getFormatSize() {
            return SIZE;
        }

        public MetaMetaFormat(long hkeys, long columns) {
            type = 0;
            version = 0;
            this.columns = columns;
            this.hkeys = hkeys;
            crc32 = 0;
            for (int i = 0; i < PAD_BYTES; i++) {
                unused[i] = 0;
            }
        }

        public MetaMetaFormat(byte[] meta) {
            assert meta.length == SIZE;
            type = unpackInt(meta, 0);
            version = unpackInt(meta, 4);
            hkeys = unpackLong(meta, 8);
            columns = unpackLong(meta, 16);
            crc32 = unpackLong(meta, 24);
            CRC32 crc = new CRC32();
            crc.update(meta, 0, 24);
            assert crc32 == crc.getValue();
        }

        public long getHKeys() {
            return hkeys;
        }

        public long getColumns() {
            return columns;
        }

        @Override
        public byte[] serialize() {
            byte[] meta = new byte[SIZE];
            packInt(meta, 0, type);
            packInt(meta, 4, version);
            packLong(meta, 8, hkeys);
            packLong(meta, 16, columns);
            CRC32 crc = new CRC32();
            crc.update(meta, 0, 24);
            long crc32 = crc.getValue();
            packLong(meta, 24, crc32);
            System.arraycopy(unused, 0, meta, 32, PAD_BYTES);
            return meta;
        }

        private static final int SIZE = 64;
        private final int PAD_BYTES = 32;
        private final int type;
        private final int version;
        private final long hkeys;
        private final long columns;
        private final long crc32;
        private final byte[] unused = new byte[PAD_BYTES];
    }

    private TreeMap<Integer, IColumnDescriptor> hkeyMap;
    private TreeMap<Long, IColumnDescriptor> columnMap;
}
