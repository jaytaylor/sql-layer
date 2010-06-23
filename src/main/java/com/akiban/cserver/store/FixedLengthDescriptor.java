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
public class FixedLengthDescriptor extends IColumnDescriptor {

    public FixedLengthDescriptor(String prefix, String schemaName, String tableName,
            String columnName, int tableId, int ordinal, int fieldSize,
            int fieldCount) {
        assert (schemaName.length() + tableName.length() + columnName.length())
                <= (3 * MAX_NAME_BYTES);

        if(prefix.equals("")) {
            prefix.equals("./");
        } else {
            prefix += "/";
        }

        this.dataPath = prefix;
        this.tableId = tableId;
        this.ordinal = ordinal;
        size = fieldSize;
        count = fieldCount;
        type = 0;
        version = 0;
        for (int i = 0; i < PAD_BYTES; i++) {
            unused[i] = 0;
        }
        crc32 = 0;

        schema = schemaName;
        table = tableName;
        column = columnName;
        for (int i = 0; i < MAX_NAME_BYTES; i++) {
            schemaBytes[i] = tableBytes[i] = columnBytes[i] = 0;
        }
        //System.out.println("schemaName" + schemaName.length());
        System.arraycopy(schemaName.getBytes(), 0, schemaBytes, 0, schemaName
                .length());
        System.arraycopy(tableName.getBytes(), 0, tableBytes, 0, tableName
                .length());
        System.arraycopy(columnName.getBytes(), 0, columnBytes, 0, columnName
                .length());

        file = new File(this.dataPath+schema+table+column);
        assert file.exists();
    }

    
    public FixedLengthDescriptor(String prefix, byte[] meta) {
        assert meta.length == SIZE;
        int offset = 0;

        if(prefix.equals("")) {
            prefix.equals("./");
        } else {
            prefix += "/";
        }

        dataPath = prefix;

        type = unpackInt(meta, offset);
        assert type == 0;
        version = unpackInt(meta, offset + 4);
        tableId = unpackInt(meta, offset + 8);
        ordinal = unpackInt(meta, offset + 12);
        size = unpackInt(meta, offset + 16);
        count = unpackLong(meta, offset + 20);

        for (int i = 0; i < MAX_NAME_BYTES; i++) {
            schemaBytes[i] = tableBytes[i] = columnBytes[i] = 0;
        }

        System.arraycopy(meta, offset + 28, schemaBytes, 0, MAX_NAME_BYTES);
        System.arraycopy(meta, offset + 92, tableBytes, 0, MAX_NAME_BYTES);
        System.arraycopy(meta, offset + 156, columnBytes, 0, MAX_NAME_BYTES);

        int schemaSize = meta[offset + 220] & 0xff;
        int tableSize = meta[offset + 221] & 0xff;
        int columnSize = meta[offset + 222] & 0xff;
        // skip 223

        schema = new String(schemaBytes, 0, schemaSize);
        table = new String(tableBytes, 0, tableSize);
        column = new String(columnBytes, 0, columnSize);

        crc32 = unpackLong(meta, offset + 224);
        CRC32 crc = new CRC32();
        crc.update(meta, 0, offset + 224);
        assert crc32 == crc.getValue();

        for (int i = 0; i < PAD_BYTES; i++) {
            unused[i] = 0;
        }

        file = new File(dataPath+schema+table+column);
        assert file.exists();
    }
    
    @Override
    public byte[] serialize() {
        byte[] meta = new byte[SIZE];
        int offset = 0;
        packInt(meta, offset, type);
        packInt(meta, offset + 4, version);
        packInt(meta, offset + 8, tableId);
        packInt(meta, offset + 12, ordinal);
        packInt(meta, offset + 16, size);
        packLong(meta, offset + 20, count);

        System.arraycopy(schemaBytes, 0, meta, offset + 28, (byte) schema
                .length());
        System.arraycopy(tableBytes, 0, meta, offset + 92, (byte) table
                .length());
        System.arraycopy(columnBytes, 0, meta, offset + 156, (byte) column
                .length());

        meta[offset + 220] = (byte) schema.length();
        meta[offset + 221] = (byte) table.length();
        meta[offset + 222] = (byte) column.length();
        // skip 223

        CRC32 crc = new CRC32();
        crc.update(meta, 0, offset + 224);
        long crc32 = crc.getValue();
        packLong(meta, offset + 224, crc32);

        System.arraycopy(unused, 0, meta, offset + 232, PAD_BYTES);
        return meta;
    }

    public long getId() {
        return (long) tableId << 32 | ordinal;
    }

    public String getFileName() {
        return dataPath + schema + table + column;
    }

    public FieldArray createFieldArray() throws FileNotFoundException,
            IOException {
       // File file = new File(prefix+schema+table+column);
        assert file.exists();
        FieldArray array = new FixedLengthArray(file, size);
//        System.out.println("fixed length descriptor: "+dataPath+schema+table+column
//                +", array. = "+ array.getColumnSize()+", size = " + size+", count = "+
//         count);
        assert array.getColumnSize() == (size * count);
        return array;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    public int getTableId() {
        return tableId;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public long getFieldCount() {
        return count;
    }

    public int getFieldSize() {
        return size;
    }

    private static final int PAD_BYTES = 792;

    private final File file;
    private final String dataPath;
    private final String schema;
    private final String table;
    private final String column;

    private final int type;
    private final int version;
    private final int tableId;
    private final int ordinal;
    private final int size;
    private final long count;
    private final byte[] schemaBytes = new byte[MAX_NAME_BYTES];
    private final byte[] tableBytes = new byte[MAX_NAME_BYTES];
    private final byte[] columnBytes = new byte[MAX_NAME_BYTES];
    private final long crc32;
    private final byte[] unused = new byte[PAD_BYTES];
}
