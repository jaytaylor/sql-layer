/**
 * 
 */
package com.akiban.cserver.store;

import java.io.FileNotFoundException;
import java.io.IOException;


/**
 * @author percent
 * 
 */
public abstract class IColumnDescriptor extends IFormat {
    
    public static IColumnDescriptor create(String prefix,
            String schemaName, String tableName, String columnName,
            int tableId, int ordinal, int fieldSize, int fieldCount) {
        return new FixedLengthDescriptor(prefix, schemaName, tableName, columnName,
                tableId, ordinal, fieldSize, fieldCount);
    }
    
    public static IColumnDescriptor create(String prefix,
            String schemaName, String tableName, int tableId, int fieldCount) {
        return new HKeyDescriptor(prefix, schemaName, tableName,
                tableId, fieldCount);
    }

    public static IColumnDescriptor create(String prefix, byte[] meta) {
        int type = IFormat.unpackInt(meta, 0);
        assert type == FIXED || type == HKEY;
        if(type == FIXED) {
            return new FixedLengthDescriptor(prefix, meta);
        } else {
            return new HKeyDescriptor(prefix, meta);
        }        
    }

    public static int getFormatSize() {
        return IColumnDescriptor.SIZE;
    }
    
    public abstract FieldArray createFieldArray() throws FileNotFoundException, IOException;
    public abstract String getSchema();
    public abstract String getTable();
    public abstract String getColumn();
    public abstract int getTableId();
    public abstract long getId();
    public abstract int getOrdinal();
    public abstract int getFieldSize();
    public abstract long getFieldCount();

    protected static final int MAX_NAME_BYTES = 64;
    protected static final int SIZE = 1024;
    protected static final int FIXED = 0;
    protected static final int HKEY = 1;
    protected static final int VARIABLE = 2;

}
