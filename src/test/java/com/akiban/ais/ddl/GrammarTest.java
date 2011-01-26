package com.akiban.ais.ddl;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.SchemaFactory;
import org.junit.Test;

import static junit.framework.Assert.*;

public class GrammarTest
{
    @Test
    public void testBug695066_1() throws Exception
    {
        UserTable table = userTable("CREATE TABLE t1(c1 TINYINT AUTO_INCREMENT NULL KEY) AUTO_INCREMENT=10;");
        assertNotNull(table.getPrimaryKey());
        Index index = table.getPrimaryKey().getIndex();
        assertNotNull(index);
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
    }

    @Test
    public void testBug695066_2() throws Exception
    {
        UserTable table = userTable("CREATE TABLE t1(c1 TINYINT AUTO_INCREMENT NULL PRIMARY KEY) AUTO_INCREMENT=10;");
        assertNotNull(table.getPrimaryKey());
        Index index = table.getPrimaryKey().getIndex();
        assertNotNull(index);
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
    }

    @Test
    public void testBug695066_3() throws Exception
    {
        UserTable table = userTable("CREATE TABLE t1(c1 TINYINT AUTO_INCREMENT NULL UNIQUE KEY) AUTO_INCREMENT=10;");
        Index index = table.getIndex("c1");
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(index.isUnique());
        // We should have generated our own PK
        assertNull(table.getPrimaryKey());
        assertNotNull(table.getPrimaryKeyIncludingInternal());
        index = table.getPrimaryKeyIncludingInternal().getIndex();
        assertNotNull(index);
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
        assertEquals(Column.AKIBAN_PK_NAME, table.getPrimaryKeyIncludingInternal().getColumns().get(0).getName());
    }

    @Test
    public void testBug695066_4() throws Exception
    {
        userTable("CREATE TABLE t1(c1 DEC NULL, c2 DEC NULL);");
    }

    private UserTable userTable(String tableDeclaration) throws Exception
    {
        String ddl = String.format("use schema; %s", tableDeclaration);
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
        return rowDefCache.getRowDef("schema.t1").userTable();
    }

    private static final SchemaFactory SCHEMA_FACTORY = new SchemaFactory();
}
