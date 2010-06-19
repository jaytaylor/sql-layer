/**
 * 
 */
package com.akiban.cserver.store;

/**
 * @author percent
 * 
 */
public class VWriterInfo {

    public VWriterInfo(int columnSize) {
        this.columnSize = columnSize;
        this.count = 0;
    }

    public VWriterInfo(String columnName, String tableName,
            String schemaName, int tableId, int ordinal) {
        this.tableId = tableId;
        this.ordinal = ordinal;
        this.columnSize = 0;
        this.count = 0;
        this.columnName = columnName;
        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    public VWriterInfo() {
        this.columnSize = 0;
        this.count = 0;
    }

    public void incrementCount() {
        count++;
    }

    public void setSize(int size) {
        if (0 == columnSize) {
            columnSize = size;
        }
    }

    public int getSize() {
        return columnSize;
    }

    public int getCount() {
        return count;
    }

    public int getTableId() {
        return tableId;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    private int tableId;
    private int ordinal;
    private int columnSize;
    private int count;
    private String schemaName;
    private String tableName;
    private String columnName;

}
