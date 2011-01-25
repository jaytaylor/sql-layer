package com.akiban.cserver.itests;

import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.*;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.ResolutionException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.ddl.IndexAlterException;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.ScanAllRequest;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public final class IndexesTest extends ApiTestBase {
    private AkibaInformationSchema createAISWithTable(TableId id) throws NoSuchTableException, ResolutionException {
        ddl().resolveTableId(id);
        Integer tableId = id.getTableId(null);
        TableName tname = ddl().getTableName(id);
        String schemaName = tname.getSchemaName();
        String tableName = tname.getTableName();

        Table curTable = ddl().getAIS(session).getTable(tname);
        AkibaInformationSchema ais = new AkibaInformationSchema();
        UserTable.create(ais, schemaName, tableName, tableId);
        
        return ais;
    }

    private Index addIndexToAIS(AkibaInformationSchema ais, String sname, String tname, String iname, 
            String[] refColumns, boolean isUnique) {
        Table table = ais.getTable(sname, tname);
        Table curTable = ddl().getAIS(session).getTable(sname, tname);
        Index index = Index.create(ais, table, iname, -1, isUnique, isUnique ? "UNIQUE" : "KEY");

        if(refColumns != null) {
            int pos = 0;
            for (String colName : refColumns) {
                Column col = curTable.getColumn(colName);
                Column refCol = Column.create(table, col.getName(), col.getPosition(), col.getType());
                refCol.setTypeParameter1(col.getTypeParameter1());
                refCol.setTypeParameter2(col.getTypeParameter2());
                Integer indexedLen = col.getMaxStorageSize().intValue();
                index.addColumn(new IndexColumn(index, refCol, pos++, true, indexedLen));
            }
        }
        
        return index;
    }
    
    private List<Index> getAllIndexes(AkibaInformationSchema ais)
    {
        ArrayList<Index> indexes = new ArrayList<Index>();
        for(UserTable tbl : ais.getUserTables().values()) {
            indexes.addAll(tbl.getIndexes());
        }
        return indexes;
    }

    
    /*
     * Test DDL.createIndexes() API 
     */
    
    public void createIndexNoIndexes() throws InvalidOperationException {
        // Passing an empty list should work
        ArrayList<Index> indexes = new ArrayList<Index>();
        ddl().createIndexes(session, indexes);
    }
    
    @Test(expected=IndexAlterException.class) 
    public void createIndexInvalidTable() throws InvalidOperationException {
        TableId tId = createTable("test", "t", "id int primary key");
        // Attempt to add index to unknown table
        AkibaInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "index", null, false);
        ddl().getAIS(session).getUserTables().remove(new TableName("test", "t"));
        ddl().createIndexes(session, getAllIndexes(ais));
    }
    
    @Test(expected=IndexAlterException.class) 
    public void createIndexMultipleTables() throws InvalidOperationException {
        AkibaInformationSchema ais = new AkibaInformationSchema();
        UserTable.create(ais, "test", "t1", 1);
        UserTable.create(ais, "test", "t2", 1);
        // Attempt to add indexes to multiple tables
        addIndexToAIS(ais, "test", "t1", "index", null, false);
        addIndexToAIS(ais, "test", "t2", "index", null, false);
        ddl().createIndexes(session, getAllIndexes(ais));
    }

    @Test(expected=IndexAlterException.class) 
    public void createIndexInvalidTableId() throws InvalidOperationException {
        TableId tId = createTable("test", "t", "id int primary key");
        // Attempt to add to unknown table id
        AkibaInformationSchema ais = createAISWithTable(tId);
        ais.getUserTables().values().iterator().next().setTableId(-1);
        addIndexToAIS(ais, "test", "t", "id", null, false);
        ddl().createIndexes(session, getAllIndexes(ais));
    }
    
    @Test(expected=IndexAlterException.class) 
    public void createIndexPrimaryKey() throws InvalidOperationException {
        TableId tId = createTable("test", "atable", "id int");
        // Attempt to a primary key
        AkibaInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "atable", "PRIMARY", new String[]{"id"}, false);
        ddl().createIndexes(session, getAllIndexes(ais));
    }
    
    @Test(expected=IndexAlterException.class) 
    public void createIndexDuplicateIndexName() throws InvalidOperationException {
        TableId tId = createTable("test", "t", "id int primary key");
        // Attempt to add duplicate index name
        AkibaInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "PRIMARY", new String[]{"id"}, false);
        ddl().createIndexes(session, getAllIndexes(ais));
    }
    
    @Test(expected=IndexAlterException.class) 
    public void createIndexWrongColumName() throws InvalidOperationException {
        TableId tId = createTable("test", "t", "id int primary key");
        // Attempt to add duplicate index name
        AkibaInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getTable("test", "t");
        Table curTable = ddl().getAIS(session).getTable("test", "t");
        Index index = Index.create(ais, table, "id", -1, false, "KEY");
        Column refCol = Column.create(table, "foo", 0, Types.INT);
        index.addColumn(new IndexColumn(index, refCol, 0, true, 0));
        ddl().createIndexes(session, getAllIndexes(ais));
    }
  
    @Test(expected=IndexAlterException.class) 
    public void createIndexWrongColumType() throws InvalidOperationException {
        TableId tId = createTable("test", "t", "id int primary key");
        // Attempt to add duplicate index name
        AkibaInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getTable("test", "t");
        Table curTable = ddl().getAIS(session).getTable("test", "t");
        Index index = Index.create(ais, table, "id", -1, false, "KEY");
        Column refCol = Column.create(table, "id", 0, Types.BLOB);
        index.addColumn(new IndexColumn(index, refCol, 0, true, 0));
        ddl().createIndexes(session, getAllIndexes(ais));
    }
    
    
    /* 
     * Test creating various types of indexes
     */
    
    @Test
    public void createIndexSimple() throws InvalidOperationException {
        TableId tId = createTable("test", "t", "id int primary key, name varchar(255)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session, createNewRow(tId, 1, "bob"));
        dml().writeRow(session, createNewRow(tId, 2, "jim"));
        expectRowCount(tId, 2);
        
        // Create non-unique index on varchar
        AkibaInformationSchema ais = createAISWithTable(tId); 
        addIndexToAIS(ais, "test", "t", "name", new String[]{"name"}, false);
        ddl().createIndexes(session, getAllIndexes(ais));
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `name` varchar(255), PRIMARY KEY(`id`), KEY `name`(`name`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 2, rows.size());
    }
    
    @Test
    public void createIndexMiddleOfGroup() throws InvalidOperationException {
        TableId cId = createTable("coi", "c", "cid int key, name varchar(32)");
        TableId oId = createTable("coi", "o", "oid int key, c_id int, tag varchar(32), CONSTRAINT __akiban_fk_c FOREIGN KEY __akiban_fk_c (c_id) REFERENCES c(cid)");
        TableId iId = createTable("coi", "i", "iid int key, o_id int, idesc varchar(32), CONSTRAINT __akiban_fk_i FOREIGN KEY __akiban_fk_i (o_id) REFERENCES o(oid)");
        
        // One customer 
        expectRowCount(cId, 0);
        dml().writeRow(session, createNewRow(cId, 1, "bob"));
        expectRowCount(cId, 1);
        
        // Two orders
        expectRowCount(oId, 0);
        dml().writeRow(session, createNewRow(oId, 1, 1, "supplies"));
        dml().writeRow(session, createNewRow(oId, 2, 1, "random"));
        expectRowCount(oId, 2);
        
        // Two/three items per order
        expectRowCount(iId, 0);
        dml().writeRow(session, createNewRow(iId, 1, 1, "foo"));
        dml().writeRow(session, createNewRow(iId, 2, 1, "bar"));
        dml().writeRow(session, createNewRow(iId, 3, 2, "zap"));
        dml().writeRow(session, createNewRow(iId, 4, 2, "fob"));
        dml().writeRow(session, createNewRow(iId, 5, 2, "baz"));
        expectRowCount(iId, 5);
        
        // Create index on an varchar (note: in the "middle" of a group, shifts IDs after, etc)
        AkibaInformationSchema ais = createAISWithTable(oId);
        addIndexToAIS(ais, "coi", "o", "tag", new String[]{"tag"}, false);
        ddl().createIndexes(session, getAllIndexes(ais));
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `coi`.`o`(`oid` int, `c_id` int, `tag` varchar(32), PRIMARY KEY(`oid`), KEY `tag`(`tag`), CONSTRAINT `__akiban_fk_c` FOREIGN KEY `__akiban_fk_c`(`c_id`) REFERENCES `c`(`cid`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session).getUserTable("coi", "o")));
        
        // Get all customers
        List<NewRow> rows = scanAll(new ScanAllRequest(cId, null));
        assertEquals("Customer rows scanned", 1, rows.size());
        
        // Get all orders
        rows = scanAll(new ScanAllRequest(oId, null));
        assertEquals("Order rows scanned", 2, rows.size());
        
        // Get all items
        rows = scanAll(new ScanAllRequest(iId, null));
        assertEquals("Item rows scanned", 5, rows.size());
    }
    
    @Test
    public void createCompoundIndex() throws InvalidOperationException {
        TableId tId = createTable("test", "t", "id int primary key, first varchar(255), last varchar(255)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session, createNewRow(tId, 1, "foo", "bar"));
        dml().writeRow(session, createNewRow(tId, 2, "zap", "snap"));
        dml().writeRow(session, createNewRow(tId, 3, "baz", "fob"));
        expectRowCount(tId, 3);
        
        // Create non-unique compound index on two varchars
        AkibaInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "name", new String[]{"first","last"}, false);
        ddl().createIndexes(session, getAllIndexes(ais));
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `first` varchar(255), `last` varchar(255), PRIMARY KEY(`id`), KEY `name`(`first`, `last`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 3, rows.size());
    }
    
    @Test
    public void createUniqueIndex() throws InvalidOperationException {
        TableId tId = createTable("test", "t", "id int primary key, state char(2)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session, createNewRow(tId, 1, "IA"));
        dml().writeRow(session, createNewRow(tId, 2, "WA"));
        dml().writeRow(session, createNewRow(tId, 3, "MA"));
        expectRowCount(tId, 3);
        
        // Create unique index on a char(2)
        AkibaInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "state", new String[]{"state"}, true);
        ddl().createIndexes(session, getAllIndexes(ais));
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `state` char(2), PRIMARY KEY(`id`), UNIQUE `state`(`state`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 3, rows.size());
    }
    
    @Test
    public void createMultipleIndexes() throws InvalidOperationException {
        TableId tId = createTable("test", "t", "id int primary key, otherId int, price decimal(10,2)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session, createNewRow(tId, 1, 1337, "10.50"));
        dml().writeRow(session, createNewRow(tId, 2, 5000, "10.50"));
        dml().writeRow(session, createNewRow(tId, 3, 47000, "9.99"));
        expectRowCount(tId, 3);
        
        // Create unique index on a int, non-unique index on decimal
        AkibaInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "otherId", new String[]{"otherId"}, true);
        addIndexToAIS(ais, "test", "t", "price", new String[]{"price"}, false);
        ddl().createIndexes(session, getAllIndexes(ais));
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `otherId` int, `price` decimal(10, 2), PRIMARY KEY(`id`), UNIQUE `otherId`(`otherId`), KEY `price`(`price`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 3, rows.size());
    }
}
