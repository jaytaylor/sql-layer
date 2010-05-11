package com.akiban.cserver.store;

import java.util.List;
import java.io.File;

import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

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

    /**
     * @see com.akiban.cserver.store.Store#writeRowForBulkLoad(com.persistit.Exchange,
     *      com.akiban.cserver.RowDef, com.akiban.cserver.RowData, int[],
     *      com.akiban.cserver.FieldDef[][], java.lang.Object[][])
     */
    @Override
    public int writeRowForBulkLoad(Exchange hEx, 
                                   RowDef rowDef,
                                   RowData rowData, 
                                   int[] ordinals, 
                                   FieldDef[][] fieldDefs,
                                   Object[][] hKey) 
        throws Exception
    {
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

        /*
         * Go through each column in this row and ensure that a file exists for that column. For
         * now, we have 1 file per column by default. If a file does not exist, then create it.
         * @todo: for now, the name used per file is the column name. this obviously needs to be
         * changed since there is nothing wrong with having multiple columns with the same name.
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
                }
                /* insert the data */
            }
        }

        return 0;
    }

    @Override
    public void updateTableStats(RowDef rowDef, long rowCount) 
        throws PersistitException, StoreException
    {
        hstore.updateTableStats(rowDef, rowCount);
    }

    private Store hstore;

    static String datapath = "/tmp/chunkserver_data";

}
