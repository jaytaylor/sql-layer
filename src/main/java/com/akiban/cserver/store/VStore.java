package com.akiban.cserver.store;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.File;
import java.io.FileOutputStream;

import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;
import com.akiban.vstore.ColumnArray;
import com.akiban.vstore.ColumnDescriptor;

/**
 * @author percent
 * @author posulliv
 */
public class VStore 
    implements Store 
{

    public void setHStore(Store hstore) 
    {
        this.hstore = hstore;
    }

    public Store getHStore() 
    {
        return hstore;
    }

    /**
     * @see com.akiban.cserver.store.Store#deleteRow(com.akiban.cserver.RowData)
     */
    @Override
    public int deleteRow(RowData rowData) throws Exception 
    {
        return hstore.deleteRow(rowData);
    }

    /**
     * @see com.akiban.cserver.store.Store#dropSchema(java.lang.String)
     */
    @Override
    public int dropSchema(String schemaName) throws Exception 
    {
        return hstore.dropSchema(schemaName);
    }

    /** 
     * @see com.akiban.cserver.store.Store#dropTable(int)
     */
    @Override
    public int dropTable(int rowDefId) throws Exception 
    {
        return hstore.dropTable(rowDefId);
    }

    /**
     * @see com.akiban.cserver.store.Store#fetchRows(java.lang.String,
     *      java.lang.String, java.lang.String, java.lang.Object,
     *      java.lang.Object, java.lang.String)
     */
    @Override
    public List<RowData> fetchRows(String schemaName, 
                                   String tableName,
                                   String columnName, 
                                   Object least, 
                                   Object greatest,
                                   String leafTableName) 
        throws Exception 
    {
        return hstore.fetchRows(schemaName, 
                                tableName, 
                                columnName, 
                                least,
                                greatest, 
                                leafTableName);
    }

    /**
     * @see com.akiban.cserver.store.Store#getAutoIncrementValue(int)
     */
    @Override
    public long getAutoIncrementValue(int rowDefId) throws Exception 
    {
        return hstore.getAutoIncrementValue(rowDefId);
    }

    /**
     * @see com.akiban.cserver.store.Store#getCurrentRowCollector(int)
     */
    @Override
    public RowCollector getCurrentRowCollector(int tableId) 
    {
        return hstore.getCurrentRowCollector(tableId);
    }

    /**
     * @see com.akiban.cserver.store.Store#getRowCount(boolean,
     *      com.akiban.cserver.RowData, com.akiban.cserver.RowData, byte[])
     */
    @Override
    public long getRowCount(boolean exact, 
                            RowData start, 
                            RowData end,
                            byte[] columnBitMap) 
        throws Exception
    {
        return hstore.getRowCount(exact, start, end, columnBitMap);
    }

    /**
     * @see com.akiban.cserver.store.Store#getRowDefCache()
     */
    @Override
    public RowDefCache getRowDefCache() 
    {
        return hstore.getRowDefCache();
    }

    /**
     * @see com.akiban.cserver.store.Store#getTableStatistics(int)
     */
    @Override
    public TableStatistics getTableStatistics(int tableId) throws Exception 
    {
        return hstore.getTableStatistics(tableId);
    }

    /**
     * @see com.akiban.cserver.store.Store#isVerbose()
     */
    @Override
    public boolean isVerbose() 
    {
        return hstore.isVerbose();
    }

    /**
     * @see com.akiban.cserver.store.Store#newRowCollector(int, int, int,
     *      com.akiban.cserver.RowData, com.akiban.cserver.RowData, byte[])
     */
    @Override
    public RowCollector newRowCollector(int rowDefId, 
                                        int indexId,
                                        int scanFlags, 
                                        RowData start, 
                                        RowData end, 
                                        byte[] columnBitMap)
        throws Exception 
    {
        return hstore.newRowCollector(rowDefId, 
                                      indexId, 
                                      scanFlags, 
                                      start, 
                                      end,
                                      columnBitMap);
    }

    /**
     * @see com.akiban.cserver.store.Store#setOrdinals()
     */
    @Override
    public void setOrdinals() throws Exception 
    {
        hstore.setOrdinals();
    }

    /**
     * @see com.akiban.cserver.store.Store#setVerbose(boolean)
     */
    @Override
    public void setVerbose(boolean verbose) 
    {
    }

    /**
     * @see com.akiban.cserver.store.Store#shutDown()
     */
    @Override
    public void shutDown() throws Exception 
    {
    }

    /**
     * @see com.akiban.cserver.store.Store#startUp()
     */
    @Override
    public void startUp() throws Exception 
    {
    }

    /**
     * @see com.akiban.cserver.store.Store#truncateTable(int)
     */
    @Override
    public int truncateTable(int rowDefId) throws Exception 
    {
        return hstore.truncateTable(rowDefId);
    }

    /**
     * @see com.akiban.cserver.store.Store#updateRow(com.akiban.cserver.RowData,
     *      com.akiban.cserver.RowData)
     */
    @Override
    public int updateRow(RowData oldRowData, RowData newRowData)
        throws Exception 
    {
        return hstore.updateRow(oldRowData, newRowData);
    }

    /**
     * @see com.akiban.cserver.store.Store#writeRow(com.akiban.cserver.RowData)
     */
    @Override
    public int writeRow(RowData rowData) throws Exception 
    {
        return hstore.writeRow(rowData);
    }

    public void constructColumnDescriptors()
    {
        columnArrays = new ArrayList<ColumnArray>();
        columnDescriptors = new ArrayList<ColumnDescriptor>();
        for (Map.Entry<String, String> entry : columnList.entrySet()) {
            try {
                File columnData = new File(entry.getValue());
                ColumnArray colArr = new ColumnArray(columnData);
                columnArrays.add(colArr);
                ColumnInfo info = columnInfo.get(entry.getKey());
                ColumnDescriptor descrip = new ColumnDescriptor(info.getTableName(),
                                                                info.getColumnName(),
                                                                info.getSize(), 
                                                                info.getCount());
                descrip.setColumnArray(colArr);
                columnDescriptors.add(descrip);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public List<ColumnDescriptor> getColumnDescriptors()
    {
        return columnDescriptors;
    }

    public List<ColumnArray> getColumnArrays()
    {
        return columnArrays;
    }

    /**
     * @see com.akiban.cserver.store.Store#writeRowForBulkLoad(com.persistit.Exchange,
     *      com.akiban.cserver.RowDef, com.akiban.cserver.RowData, int[],
     *      com.akiban.cserver.FieldDef[][], java.lang.Object[][])
     */
    @Override
    public int writeRowForBulkLoad(final Exchange hEx, 
                                   final RowDef rowDef,
                                   final RowData rowData, 
                                   final int[] ordinals, 
                                   FieldDef[][] fieldDefs,
                                   Object[][] hKeyValues) 
        throws Exception
    {
        final Key hkey = constructHKey(rowDef, ordinals, fieldDefs, hKeyValues);

        /*
         * First check if a directory exists for this table. If not, then create it.
         */
        String tableName = rowDef.getTableName();
        String tableDirectory = datapath + "/" + tableName;
        File tableData = new File(tableDirectory);
        if (! tableData.exists()) {
            boolean ret = tableData.mkdir();
            if (! ret) {
                throw new Exception(); /* probably permission issue */
            }
        }

        /* column for the hkey. */
        String hkeyColumnPath = tableDirectory + "/hkeyColumn";
        File hkeyColumn = new File(hkeyColumnPath);
        if (! hkeyColumn.exists()) {
            boolean ret = hkeyColumn.createNewFile();
            if (! ret) {
                throw new Exception();
            }
            FileOutputStream fout = new FileOutputStream(hkeyColumn, true);
            fout.write(hkey.getEncodedBytes()); /* write the key's bytes to disk */
        }

        /*
         * Go through each column in this row and ensure that a file exists for that column. For
         * now, we have 1 file per column by default. If a file does not exist, then create it.
         * @todo: for now, the name used per file is the column name. Need to discuss if this needs
         * to be changed or not.
         */
        for (int i = 0; i < fieldDefs.length; i++) {
            FieldDef[] tableFieldDefs = fieldDefs[i];
            for (int j = 0; j < tableFieldDefs.length; j++) {
                FieldDef field = tableFieldDefs[j];
                String columnName = field.getName();
                String columnFileName = tableDirectory + "/" + columnName;
                File columnData = new File(columnFileName);
                if (! columnData.exists()) {
                    boolean ret = columnData.createNewFile();
                    if (! ret) {
                        throw new Exception();
                    }
                    columnList.put(columnName, columnFileName);
                    ColumnInfo info = new ColumnInfo(columnName, tableName);
                    columnInfo.put(columnName, info);
                }
                ColumnInfo info = columnInfo.get(columnName); /* @todo: temporary only */
                /* insert the data */
                final long locationAndSize = rowDef.fieldLocation(rowData, j);
                if (0 == locationAndSize) {
                    /* NULL field. @todo: how do we handle NULL's in the V store? */
                }
                int offset = (int) locationAndSize;
                int size = (int) (locationAndSize >>> 32);
                byte[] bytes = rowData.getBytes();
                FileOutputStream fout = new FileOutputStream(columnData, true);
                fout.write(bytes, offset, size);

                info.incrementCount();
                info.setSize(size);
                columnInfo.put(columnName, info);
            }
        }

        return 0;
    }

    private Key constructHKey(final RowDef rowDef,
                              final int[] ordinals,
                              final FieldDef[][] fieldDefs,
                              final Object[][] hKeyValues)
        throws Exception
    {
        final Key hKey = new Key(((PersistitStore) hstore).getDb());
        hKey.clear();
        for (int i = 0; i < hKeyValues.length; i++) {
            hKey.append(ordinals[i]);
            Object[] tableHKeyValues = hKeyValues[i];
            FieldDef[] tableFieldDefs = fieldDefs[i];
            for (int j = 0; j < tableHKeyValues.length; j++) {
                tableFieldDefs[j].getEncoding().toKey(tableFieldDefs[j], 
                                                      tableHKeyValues[j],
                                                      hKey);
            }
        }
        return hKey;
    }

    @Override
    public void updateTableStats(RowDef rowDef, long rowCount) 
        throws PersistitException, StoreException
    {
        hstore.updateTableStats(rowDef, rowCount);
    }

    public static void setDataPath(final String path)
    {
        datapath = path;
    }

    /*
     * Temporary class only being used for testing purposes right now to carry metadata about
     * columns. Once the metadata for column is actually stored in some kind of header on disk, we
     * shouldn't need this class anymore.
     */
    class ColumnInfo
    {
        public ColumnInfo(long columnSize)
        {
            this.columnSize = columnSize;
            this.count = 0;
        }

        public ColumnInfo(String columnName, String tableName)
        {
            this.columnSize = 0;
            this.count = 0;
            this.columnName = columnName;
            this.tableName = tableName;
        }

        public ColumnInfo()
        {
            this.columnSize = 0;
            this.count = 0;
        }

        public void incrementCount()
        {
            count++;
        }

        public void setSize(long size)
        {
            if (0 == columnSize) {
                columnSize = size;
            }
        }

        public long getSize()
        {
            return columnSize;
        }

        public long getCount()
        {
            return count;
        }

        public String getTableName()
        {
            return tableName;
        }

        public String getColumnName()
        {
            return columnName;
        }

        private long columnSize;
        private long count;
        private String tableName;
        private String columnName;
    }

    private Store hstore;

    static String datapath = "/tmp/chunkserver_data";

    private HashMap<String, String> columnList = new HashMap<String, String>();

    private HashMap<String, ColumnInfo> columnInfo = new HashMap<String, ColumnInfo>();

    private List<ColumnArray> columnArrays;

    private List<ColumnDescriptor> columnDescriptors;

}
