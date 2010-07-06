/**
 * 
 */
package com.akiban.cserver.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.CRC32;


/**
 * @author percent
 * 
 */
public class HKeyDescriptor extends IColumnDescriptor {

    public HKeyDescriptor(String prefix, String schemaName,
            String tableName, int tableId, int fieldCount) {
        assert (schemaName.length() + tableName.length()) <= (2 * MAX_NAME_BYTES);

        if (prefix.equals("")) {
            prefix.equals("./");
        } else {
            prefix += "/"; 
        }

        this.dataPath = prefix;
        this.tableId = tableId;
        count = fieldCount;
        type = 1;
        version = 0;
        for (int i = 0; i < PAD_BYTES; i++) {
            unused[i] = 0;
        }
        crc32 = 0;

        schema = schemaName;
        table = tableName;
        for (int i = 0; i < MAX_NAME_BYTES; i++) {
            schemaBytes[i] = tableBytes[i] = columnBytes[i] = 0;
        }
        System.arraycopy(schemaName.getBytes(), 0, schemaBytes, 0, schemaName
                .length());
        System.arraycopy(tableName.getBytes(), 0, tableBytes, 0, tableName
                .length());
        
        metaFile = new File(this.dataPath + schema + table +"-hkey.meta");
        dataFile = new File(this.dataPath + schema + table +"-hkey.data");
        assert metaFile.exists() : metaFile.toString();
        assert dataFile.exists() : dataFile.toString();
    }

    public HKeyDescriptor(String prefix, byte[] meta) {
        assert meta.length == SIZE;

        if (prefix.equals("")) {
            prefix.equals("./");
        } else {
            prefix += "/";
        }
        dataPath = prefix;

        type = unpackInt(meta, 0);
        assert type == 1;
        version = unpackInt(meta, 4);
        tableId = unpackInt(meta, 8);
        count = unpackLong(meta, 12);

        for (int i = 0; i < MAX_NAME_BYTES; i++) {
            schemaBytes[i] = tableBytes[i] = 0;
        }

        System.arraycopy(meta, 20, schemaBytes, 0, MAX_NAME_BYTES);
        System.arraycopy(meta, 84, tableBytes, 0, MAX_NAME_BYTES);

        int schemaSize = meta[148] & 0xff;
        int tableSize = meta[149] & 0xff;
        // skip 150, 151
        
        schema = new String(schemaBytes, 0, schemaSize);
        table = new String(tableBytes, 0, tableSize);


        crc32 = unpackLong(meta, 152);
        CRC32 crc = new CRC32();
        crc.update(meta, 0, 152);
        assert crc32 == crc.getValue();

        for (int i = 0; i < PAD_BYTES; i++) {
            unused[i] = 0;
        }
        metaFile = new File(dataPath + schema + table + "-hkey.meta");
        dataFile = new File(dataPath + schema + table + "-hkey.data");
        assert metaFile.exists() && dataFile.exists();
    }
    
    @Override
    public byte[] serialize() {
        byte[] meta = new byte[SIZE];
        packInt(meta, 0, type);
        packInt(meta, 4, version);
        packInt(meta, 8, tableId);
        packLong(meta, 12, count);

        System.arraycopy(schemaBytes, 0, meta, 20, (byte) schema
                .length());
        System.arraycopy(tableBytes, 0, meta, 84, (byte) table
                .length());

        meta[148] = (byte) schema.length();
        meta[149] = (byte) table.length();
        meta[150] = 0; // skip
        meta[151] = 0; // skip

        CRC32 crc = new CRC32();
        crc.update(meta, 0, 152);
        long crc32 = crc.getValue();
        packLong(meta, 152, crc32);

        System.arraycopy(unused, 0, meta, 160, PAD_BYTES);
        return meta;
    }

    public FieldArray createFieldArray() throws FileNotFoundException,
            IOException {
        assert metaFile.exists();
        assert dataFile.exists();
        FieldArray array = new VariableLengthArray(metaFile, dataFile);
        // System.out.println("array. = "+ array.getSize()+", size*count"+
        // size*count);
        return array;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }
    
    public int getTableId() {
        return tableId;
    }

    public String getColumn() {
        return null;
    }

    public long getId() {
        return -1;
    }

    public int getOrdinal() {
        return -1;
    }
    
    public int getFieldSize() {
        return -1;
    }

    public long getFieldCount() {
        return count;
    }

    private static final int PAD_BYTES = 864;

    private final File metaFile;
    private final File dataFile;
    private final String dataPath;
    private final String schema;
    private final String table;
    private final int type;
    private final int version;
    private final int tableId;
    private final long count;
    private final byte[] schemaBytes = new byte[MAX_NAME_BYTES];
    private final byte[] tableBytes = new byte[MAX_NAME_BYTES];
    private final byte[] columnBytes = new byte[MAX_NAME_BYTES];
    private final long crc32;
    private final byte[] unused = new byte[PAD_BYTES];
}
