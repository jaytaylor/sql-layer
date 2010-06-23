/**
 * 
 */
package com.akiban.cserver.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;


/**
 * @author percent
 * 
 */
public class TableDescriptor {
    
    public TableDescriptor(IColumnDescriptor hkey, int rootid, int tableid) {
        this.tableId = tableid;
        this.rootId = rootid;
        this.hkey = hkey;
        first = true;
        rows = 0;
        cursor = 0;
        columns = new ArrayList<IColumnDescriptor>();
        arrays = new ArrayList<FieldArray>();
    }
    
    public void add(IColumnDescriptor c) throws FileNotFoundException, IOException {        
        columns.add(c);
        arrays.add(c.createFieldArray());
        if(first) {
            rows = (int) c.getFieldCount();
            first = false;
        } else {
            assert rows == c.getFieldCount();
        }
    }

    public List<FieldArray> getFieldArrayList() {
        return arrays;
    }
    
    public void incrementCursor() {
        cursor++;
    }

    public int getTableId() {
        return tableId;
    }
    
    public int getRoot() {
        return rootId;
    }
    
    public int getRows() {
        return rows;
    }
    
    public String toString() {
        return Integer.toString(rootId)+Integer.toString(tableId);
    }

    public List<byte[]> scanKeys() throws IOException {
        FieldArray keyArray = hkey.createFieldArray();
        ArrayList<byte[]> keys = new ArrayList<byte[]>();
        boolean hasMore = true;
        while(hasMore) {
            int keySize = keyArray.getNextFieldSize();
            byte[] keyBytes = new byte[keySize];
            hasMore = keyArray.copyNextField(keyBytes, 0);
            keys.add(keyBytes);
        }
        return keys;
    }
    
    private IColumnDescriptor hkey;
    private int tableId;
    private int rootId;
    private int rows;
    private ArrayList<IColumnDescriptor> columns;
    private ArrayList<FieldArray> arrays;
    private int cursor;
    private boolean first;
}
