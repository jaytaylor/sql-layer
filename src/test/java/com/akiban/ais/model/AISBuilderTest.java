/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.akiban.ais.model.validation.AISValidationFailure;
import com.akiban.ais.model.validation.AISValidationResults;
import com.akiban.ais.model.validation.AISValidations;
import com.akiban.server.error.BranchingGroupIndexException;
import com.akiban.server.error.ErrorCode;

public class AISBuilderTest
{
    @Test
    public void testEmptyAIS()
    {
        AISBuilder builder = new AISBuilder();
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(0, ais.getUserTables().size());
        Assert.assertEquals(0, ais.getGroupTables().size());
        Assert.assertEquals(0, ais.getGroups().size());
        Assert.assertEquals(0, ais.getJoins().size());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }
    
    @Test
    public void testSingleTableNoGroups()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.basicSchemaIsComplete();
        builder.setTableTreeNamesForTest();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(1, ais.getUserTables().size());
        Assert.assertEquals(0, ais.getGroupTables().size());
        Assert.assertEquals(0, ais.getGroups().size());
        Assert.assertEquals(0, ais.getJoins().size());

        Assert.assertEquals(1, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
       
    }

    @Test
    public void testSingleTableInGroup()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addTableToGroup("group", "schema", "customer");
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(1, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(0, ais.getJoins().size());
        Group group = ais.getGroup("group");
        GroupTable groupTable = group.getGroupTable();
        Assert.assertEquals("customer$customer_id", groupTable.getColumn(0).getName());
        Assert.assertEquals("customer$customer_name", groupTable.getColumn(1).getName());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testSingleTableInGroupLongColumnNames()
    {
        final String tableName = "customer";
        final int EXTRA_CHARS = tableName.length() + 5;
        final String columnOne;
        final String columnTwo;
        {
            StringBuilder builder = new StringBuilder(tableName).append('$');
            builder.append('1');
            while (builder.length() < AISBuilder.MAX_COLUMN_NAME_LENGTH + EXTRA_CHARS)
            {
                builder.append('x');
            }
            builder.delete(0, builder.indexOf("$")+1);
            Assert.assertEquals("sanity check", builder.charAt(0), '1');
            columnOne = builder.toString();
            builder.setCharAt(0, '2');
            columnTwo = builder.toString();
        }
        
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", tableName, columnOne, 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", tableName, columnTwo, 1, "varchar", 64L, 0L, false, false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addTableToGroup("group", "schema", "customer");
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(1, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(0, ais.getJoins().size());
        Group group = ais.getGroup("group");
        GroupTable groupTable = group.getGroupTable();
        
        String groupCol1 = groupTable.getColumn(0).getName();
        String groupCol2 = groupTable.getColumn(1).getName();
        // test truncation:
        // First column should be the last AISBuilder.MAX_COLUMN_NAME_LENGTH digits of columnOne (which should be all x's)
        // Second column should be that, with the last two chars replaced with "$1"
        Assert.assertEquals(groupCol1,
                columnOne.substring(columnOne.length()-AISBuilder.MAX_COLUMN_NAME_LENGTH, columnOne.length())
        );
        for (int i=0, len=groupCol1.length(); i < len; ++i)
        {
            Assert.assertEquals('x', groupCol1.charAt(i));
        }
        Assert.assertEquals(groupCol2,
                columnOne.substring(columnOne.length()-AISBuilder.MAX_COLUMN_NAME_LENGTH, columnOne.length()-2) + "$1"
        );
        Assert.assertFalse("equal names: " + groupCol1, groupCol1.equals(groupCol2));

        Assert.assertEquals(0,
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testMultipleTableSameColumnNameLongGroupTableNames() {
        final int TABLE_COUNT = 11;
        final String SCHEMA = "schema";
        final String PARENT_NAME = "p";
        final String PARENT_PK_COL = "id";
        final String CHILD_PREFIX = "t";
        final String CHILD_JOIN_COL = "pid";

        String longName = "";
        while(longName.length() < AISBuilder.MAX_COLUMN_NAME_LENGTH) {
            longName += "x";
        }

        AISBuilder builder = new AISBuilder();
        for(int i = 0; i < TABLE_COUNT; ++i) {
            String name = i == 0 ? PARENT_NAME : CHILD_PREFIX + i;
            String pkCol = i == 0 ? PARENT_PK_COL : longName;
            builder.userTable(SCHEMA, name);
            builder.column(SCHEMA, name, pkCol, 0, "int", 0L, 0L, false, false, null, null);
            builder.index(SCHEMA, name, "PRIMARY", true, "PRIMARY");
            builder.indexColumn(SCHEMA, name, "PRIMARY", pkCol, 0, true, null);
            if(i > 0) {
                builder.column(SCHEMA, name, CHILD_JOIN_COL, 1, "int", 0L, 0L, true, false, null, null);
            }
        }
        builder.basicSchemaIsComplete();

        String groupTableName = "_" + PARENT_NAME;
        builder.createGroup(PARENT_NAME, SCHEMA, groupTableName);
        for(int i = 1; i < TABLE_COUNT; ++i) {
            String childName = CHILD_PREFIX + i;
            builder.joinTables(childName, SCHEMA, PARENT_NAME, SCHEMA, childName);
            builder.joinColumns(childName, SCHEMA, PARENT_NAME, "id", SCHEMA, childName, CHILD_JOIN_COL);
            builder.addJoinToGroup(PARENT_NAME, childName, 0);
        }
        builder.groupingIsComplete();

        GroupTable groupTable = builder.akibanInformationSchema().getGroupTable(SCHEMA, groupTableName);
        Assert.assertNotNull("Found group table", groupTable);

        for(Column column : groupTable.getColumns()) {
            int len = column.getName().length();
            Assert.assertTrue("Less or equal than max length: "+len, len <= AISBuilder.MAX_COLUMN_NAME_LENGTH);
            if (column.getUserColumn().getName().equals(longName)) {
                Assert.assertEquals("Group table column length", AISBuilder.MAX_COLUMN_NAME_LENGTH, len);
            }
        }
    }

    @Test
    public void testSingleJoinInGroup()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, "customer_id", 0, true, null);
        builder.userTable("schema", "order");
        builder.column("schema", "order", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "customer_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addJoinToGroup("group", "co", 0);
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(2, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(1, ais.getJoins().size());
        Group group = ais.getGroup("group");
        GroupTable groupTable = group.getGroupTable();
        Assert.assertEquals("customer$customer_id", groupTable.getColumn(0).getName());
        Assert.assertEquals("customer$customer_name", groupTable.getColumn(1).getName());
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testTableAndThenSingleJoinInGroup()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, "customer_id", 0, true, null);
        builder.userTable("schema", "order");
        builder.column("schema", "order", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "customer_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addTableToGroup("group", "schema", "customer");
        builder.addJoinToGroup("group", "co", 0);
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(2, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(1, ais.getJoins().size());
        Group group = ais.getGroup("group");
        GroupTable groupTable = group.getGroupTable();
        Assert.assertEquals("customer$customer_id", groupTable.getColumn(0).getName());
        Assert.assertEquals("customer$customer_name", groupTable.getColumn(1).getName());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testTwoJoinsInGroup()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, "customer_id", 0, true, null);
        builder.userTable("schema", "order");
        builder.column("schema", "order", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "customer_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "order", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "order", Index.PRIMARY_KEY_CONSTRAINT, "order_id", 0, true, null);
        builder.userTable("schema", "item");
        builder.column("schema", "item", "item_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "order_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "quantity", 2, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "item_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(3, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(2, ais.getJoins().size());
        Group group = ais.getGroup("group");
        GroupTable groupTable = group.getGroupTable();
        int c = 0;
        Assert.assertEquals("customer$customer_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("customer$customer_name", groupTable.getColumn(c++).getName());
        Assert.assertEquals("order$order_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("order$customer_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("order$order_date", groupTable.getColumn(c++).getName());
        Assert.assertEquals("item$item_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("item$order_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("item$quantity", groupTable.getColumn(c++).getName());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testTwoJoinsInGroupThenClearAndRetry()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, "customer_id", 0, true, null);
        builder.userTable("schema", "order");
        builder.column("schema", "order", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "customer_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "order", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "order", Index.PRIMARY_KEY_CONSTRAINT, "order_id", 0, true, null);
        builder.userTable("schema", "item");
        builder.column("schema", "item", "item_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "order_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "quantity", 2, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "item_id");
        builder.basicSchemaIsComplete();

        // Step 1 -- group
        builder.createGroup("group", "groupschema", "coi");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        {
            AkibanInformationSchema ais = builder.akibanInformationSchema();
            Assert.assertEquals(3, ais.getUserTables().size());
            Assert.assertEquals(1, ais.getGroupTables().size());
            Assert.assertEquals(1, ais.getGroups().size());
            Assert.assertEquals(2, ais.getJoins().size());
            Group group = ais.getGroup("group");
            GroupTable groupTable = group.getGroupTable();
            int c = 0;
            Assert.assertEquals("customer$customer_id", groupTable.getColumn(c++).getName());
            Assert.assertEquals("customer$customer_name", groupTable.getColumn(c++).getName());
            Assert.assertEquals("order$order_id", groupTable.getColumn(c++).getName());
            Assert.assertEquals("order$customer_id", groupTable.getColumn(c++).getName());
            Assert.assertEquals("order$order_date", groupTable.getColumn(c++).getName());
            Assert.assertEquals("item$item_id", groupTable.getColumn(c++).getName());
            Assert.assertEquals("item$order_id", groupTable.getColumn(c++).getName());
            Assert.assertEquals("item$quantity", groupTable.getColumn(c++).getName());
        }

        // Step 2 -- clear
        builder.clearGroupings();
        {
            AkibanInformationSchema ais = builder.akibanInformationSchema();
            Assert.assertEquals(3, ais.getUserTables().size());
            Assert.assertEquals(0, ais.getGroupTables().size());
            Assert.assertEquals(0, ais.getGroups().size());
            Assert.assertEquals(2, ais.getJoins().size());
            Assert.assertNull( ais.getGroup("group") );
        }

        // Step 3 -- regroup with different name
        builder.createGroup("group2", "groupschema", "coi");
        builder.addJoinToGroup("group2", "co", 0);
        builder.addJoinToGroup("group2", "oi", 0);
        {
            builder.groupingIsComplete();
            AkibanInformationSchema ais = builder.akibanInformationSchema();
            Assert.assertEquals(3, ais.getUserTables().size());
            Assert.assertEquals(1, ais.getGroupTables().size());
            Assert.assertEquals(1, ais.getGroups().size());
            Assert.assertEquals(2, ais.getJoins().size());
            Group group = ais.getGroup("group2");
            GroupTable groupTable = group.getGroupTable();
            int c = 0;
            Assert.assertEquals("customer$customer_id", groupTable.getColumn(c++).getName());
            Assert.assertEquals("customer$customer_name", groupTable.getColumn(c++).getName());
            Assert.assertEquals("order$order_id", groupTable.getColumn(c++).getName());
            Assert.assertEquals("order$customer_id", groupTable.getColumn(c++).getName());
            Assert.assertEquals("order$order_date", groupTable.getColumn(c++).getName());
            Assert.assertEquals("item$item_id", groupTable.getColumn(c++).getName());
            Assert.assertEquals("item$order_id", groupTable.getColumn(c++).getName());
            Assert.assertEquals("item$quantity", groupTable.getColumn(c++).getName());
        }
        
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }


    @Test
    public void testRemoval()
    {
        // Setup as in testTwoJoinsInGroup
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", "pk", "customer_id", 0, true, null);
        builder.userTable("schema", "order");
        builder.column("schema", "order", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "customer_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "order", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "order", Index.PRIMARY_KEY_CONSTRAINT, "order_id", 0, true, null);
        builder.userTable("schema", "item");
        builder.column("schema", "item", "item_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "order_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "quantity", 2, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "item_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(3, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(2, ais.getJoins().size());
        Group group = ais.getGroup("group");
        GroupTable groupTable = group.getGroupTable();
        int c = 0;
        Assert.assertEquals("customer$customer_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("customer$customer_name", groupTable.getColumn(c++).getName());
        Assert.assertEquals("order$order_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("order$customer_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("order$order_date", groupTable.getColumn(c++).getName());
        Assert.assertEquals("item$item_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("item$order_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("item$quantity", groupTable.getColumn(c++).getName());
        // Remove customer/order join
        builder.removeJoinFromGroup("group", "co");
        Assert.assertEquals(3, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(2, ais.getJoins().size());
        group = ais.getGroup("group");
        groupTable = group.getGroupTable();
        c = 0;
        Assert.assertEquals("order$order_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("order$customer_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("order$order_date", groupTable.getColumn(c++).getName());
        Assert.assertEquals("item$item_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("item$order_id", groupTable.getColumn(c++).getName());
        Assert.assertEquals("item$quantity", groupTable.getColumn(c++).getName());
        // Remove order/item join
        builder.removeJoinFromGroup("group", "oi");
        Assert.assertEquals(3, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(2, ais.getJoins().size());
        group = ais.getGroup("group");
        groupTable = group.getGroupTable();
        Assert.assertEquals(0, groupTable.getColumns().size());

        AISValidationResults results = builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS);
        
        Assert.assertEquals(3,results.failures().size());
        Iterator<AISValidationFailure> failures = results.failures().iterator();
        
        Assert.assertEquals("Table `schema`.`customer` does not belong to any group", failures.next().message());
        Assert.assertEquals("Table `schema`.`item` does not belong to any group", failures.next().message());
        Assert.assertEquals("Table `schema`.`order` does not belong to any group", failures.next().message());
        
    }

    @Test
    public void testForwardReference()
    {
        AISBuilder builder = new AISBuilder();
        // Create order
        builder.userTable("schema", "order");
        builder.column("schema", "order", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "customer_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L, false, false, null, null);
        // Create join from order to customer
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        // Create customer
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", "pk", "customer_id", 0, true, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addTableToGroup("group", "schema", "customer");
        builder.addJoinToGroup("group", "co", 0);
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(2, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(1, ais.getJoins().size());
        Group group = ais.getGroup("group");
        GroupTable groupTable = group.getGroupTable();
        Assert.assertEquals("customer$customer_id", groupTable.getColumn(0).getName());
        Assert.assertEquals("customer$customer_name", groupTable.getColumn(1).getName());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testDeleteGroupWithOneTable()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", "pk", "customer_id", 0, true, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addTableToGroup("group", "schema", "customer");
        builder.removeTableFromGroup("group", "schema", "customer");
        builder.deleteGroup("group");
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(1, ais.getUserTables().size());
        Assert.assertEquals(0, ais.getGroupTables().size());
        Assert.assertEquals(0, ais.getGroups().size());
        Assert.assertEquals(0, ais.getJoins().size());

        Assert.assertEquals(1, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testDeleteGroupWithOneJoin()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", "pk", "customer_id", 0, true, null);
        builder.userTable("schema", "order");
        builder.column("schema", "order", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "customer_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "order", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "order", "pk", "order_id", 0, true, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addTableToGroup("group", "schema", "customer");
        builder.addJoinToGroup("group", "co", 0);
        builder.removeJoinFromGroup("group", "co");
        builder.deleteGroup("group");
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(2, ais.getUserTables().size());
        Assert.assertEquals(0, ais.getGroupTables().size());
        Assert.assertEquals(0, ais.getGroups().size());
        Assert.assertEquals(1, ais.getJoins().size());

        Assert.assertEquals(2, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testMoveTreeToEmptyGroup()
    {
        AISBuilder builder = new AISBuilder();
        // Source group tables: a(b(c, d))
        builder.userTable("s", "a");
        builder.column("s", "a", "aid", 0, "int", 0L, 0L, false, false, null, null);
        builder.index("s", "a", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "a", "pk", "aid", 0, true, null);
        builder.userTable("s", "b");
        builder.column("s", "b", "bid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "b", "aid", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("s", "b", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "b", "pk", "bid", 0, true, null);
        builder.userTable("s", "c");
        builder.column("s", "c", "cid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "c", "bid", 1, "int", 0L, 0L, false, false, null, null);
        builder.userTable("s", "d");
        builder.column("s", "d", "did", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "d", "bid", 1, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("ab", "s", "a", "s", "b");
        builder.joinColumns("ab", "s", "a", "aid", "s", "b", "aid");
        builder.joinTables("bc", "s", "b", "s", "c");
        builder.joinColumns("bc", "s", "b", "bid", "s", "c", "bid");
        builder.joinTables("bd", "s", "b", "s", "d");
        builder.joinColumns("bd", "s", "b", "bid", "s", "d", "bid");
        // Source and target groups
        builder.basicSchemaIsComplete();
        builder.createGroup("source", "g", "source_table");
        builder.addJoinToGroup("source", "ab", 0);
        builder.addJoinToGroup("source", "bc", 0);
        builder.addJoinToGroup("source", "bd", 0);
        builder.createGroup("target", "g", "target_table");
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(4, ais.getUserTables().size());
        Assert.assertEquals(2, ais.getGroupTables().size());
        Assert.assertEquals(2, ais.getGroups().size());
        Assert.assertEquals(3, ais.getJoins().size());
        // Move b to target
        builder.moveTreeToEmptyGroup("s", "b", "target");
        UserTable a = ais.getUserTable("s", "a");
        List<Join> aChildren = a.getChildJoins();
        Assert.assertTrue(aChildren.isEmpty());
        UserTable b = ais.getUserTable("s", "b");
        Assert.assertSame(b, ais.getGroup("target").getGroupTable().getRoot());
        int count = 0;
        for (Join join : b.getChildJoins()) {
            if (join.getChild() == ais.getUserTable("s", "c") ||
                join.getChild() == ais.getUserTable("s", "d")) {
                count++;
            } else {
                Assert.fail();
            }
        }

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testMoveTreeToEmptyGroup_bug95()
    {
        AISBuilder builder = new AISBuilder();

        builder.userTable("s", "c");
        builder.column("s", "c", "c_id", 0, "INT", 4L, null, false, true, null, null);
        builder.index("s", "c", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "c", Index.PRIMARY_KEY_CONSTRAINT, "c_id", 0, true, null);

        builder.userTable("s", "o");
        builder.column("s", "o", "o_id", 0, "INT", 4L, null, false, true, null, null);
        builder.column("s", "o", "c_id", 1, "INT", 4L, null, false, false, null, null);
        builder.index("s", "o", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "o", Index.PRIMARY_KEY_CONSTRAINT, "o_id", 0, true, null);
        builder.index("s", "o", "customer", false, "FOREIGN KEY");
        builder.indexColumn("s", "o", "customer", "c_id", 0, false, null);
        builder.basicSchemaIsComplete();

        builder.joinTables("co", "s", "c", "s", "o");
        builder.joinColumns("co", "s", "c", "c_id", "s", "o", "c_id");

        builder.createGroup("group_01", "s", "customers");
        builder.addTableToGroup("group_01", "s", "c");
        builder.addJoinToGroup("group_01", "co", 1);
        builder.groupingIsComplete();

        
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());

        builder.createGroup("group_02", "s", "orders");
        builder.moveTreeToEmptyGroup("s", "o", "group_02");

        //Assert.assertEquals("number of joins", 0, builder.akibanInformationSchema().getJoins().size());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());

        builder.groupingIsComplete();
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testMoveTreeToNonEmptyGroup() throws Exception {
        AISBuilder builder = new AISBuilder();
        // Source group tables: a(b(c, d))
        builder.userTable("s", "a");
        builder.column("s", "a", "aid", 0, "int", 0L, 0L, false, false, null, null);
        builder.index("s", "a", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "a", "pk", "aid", 0, true, null);
        builder.userTable("s", "b");
        builder.column("s", "b", "bid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "b", "aid", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("s", "b", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "b", "pk", "bid", 0, true, null);
        builder.userTable("s", "c");
        builder.column("s", "c", "cid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "c", "bid", 1, "int", 0L, 0L, false, false, null, null);
        builder.userTable("s", "d");
        builder.column("s", "d", "did", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "d", "bid", 1, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("ab", "s", "a", "s", "b");
        builder.joinColumns("ab", "s", "a", "aid", "s", "b", "aid");
        builder.joinTables("bc", "s", "b", "s", "c");
        builder.joinColumns("bc", "s", "b", "bid", "s", "c", "bid");
        builder.joinTables("bd", "s", "b", "s", "d");
        builder.joinColumns("bd", "s", "b", "bid", "s", "d", "bid");
        // b has a candidate join to z
        builder.joinTables("bz", "s", "z", "s", "b");
        builder.joinColumns("bz", "s", "z", "zid", "s", "b", "bid");
        // Target group tables: z
        builder.userTable("s", "z");
        builder.column("s", "z", "zid", 0, "int", 0L, 0L, false, false, null, null);
        builder.index("s", "z", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "z", "pk", "zid", 0, true, null);
        // Source and target groups
        builder.basicSchemaIsComplete();
        builder.createGroup("source", "g", "source_table");
        builder.addJoinToGroup("source", "ab", 0);
        builder.addJoinToGroup("source", "bc", 0);
        builder.addJoinToGroup("source", "bd", 0);
        builder.createGroup("target", "g", "target_table");
        builder.addTableToGroup("target", "s", "z");
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(5, ais.getUserTables().size());
        Assert.assertEquals(2, ais.getGroupTables().size());
        Assert.assertEquals(2, ais.getGroups().size());
        Assert.assertEquals(4, ais.getJoins().size());
        // Move b to target
        builder.moveTreeToGroup("s", "b", "target", "bz");
        UserTable a = ais.getUserTable("s", "a");
        List<Join> aChildren = a.getChildJoins();
        Assert.assertTrue(aChildren.isEmpty());
        UserTable z = ais.getUserTable("s", "z");
        Assert.assertSame(z, ais.getGroup("target").getGroupTable().getRoot());
        Assert.assertEquals(1, z.getChildJoins().size());
        Join bz = z.getChildJoins().get(0);
        UserTable b = ais.getUserTable("s", "b");
        Assert.assertSame(b, bz.getChild());
        int count = 0;
        for (Join join : b.getChildJoins()) {
            if (join.getChild() == ais.getUserTable("s", "c") ||
                join.getChild() == ais.getUserTable("s", "d")) {
                count++;
            } else {
                Assert.fail();
            }
        }
        // AISPrinter.print(ais);

        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS);
        
        Assert.assertEquals(1, vResults.failures().size());
        AISValidationFailure fail = vResults.failures().iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_MULTIPLE_PARENTS, fail.errorCode());
    }

    @Test
    public void testInitialAutoInc()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("s", "b");
        builder.column("s", "b", "x", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "b", "y", 1, "int", 0L, 0L, false, true, null, null);
        builder.column("s", "b", "z", 2, "int", 0L, 0L, false, false, null, null);
        builder.userTableInitialAutoIncrement("s", "b", 5L);
        builder.basicSchemaIsComplete();
        builder.setTableTreeNamesForTest();
        // Check autoinc state
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        UserTable table = ais.getUserTable("s", "b");
        Assert.assertEquals(3, table.getColumns().size());
        Assert.assertEquals(table.getColumn("y"), table.getAutoIncrementColumn());
        Assert.assertEquals(null, table.getColumn("x").getInitialAutoIncrementValue());
        Assert.assertEquals(5L, table.getColumn("y").getInitialAutoIncrementValue().longValue());
        Assert.assertEquals(null, table.getColumn("z").getInitialAutoIncrementValue());

        Assert.assertEquals(1, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testInitialAutoIncNoAutoInc()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("s", "b");
        builder.column("s", "b", "x", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "b", "y", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "b", "z", 2, "int", 0L, 0L, false, false, null, null);
        builder.userTableInitialAutoIncrement("s", "b", 5L);
        builder.basicSchemaIsComplete();
        builder.setTableTreeNamesForTest();
        // Check autoinc state
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        UserTable table = ais.getUserTable("s", "b");
        Assert.assertEquals(3, table.getColumns().size());
        Assert.assertEquals(null, table.getAutoIncrementColumn());
        Assert.assertEquals(null, table.getColumn("x").getInitialAutoIncrementValue());
        Assert.assertEquals(null, table.getColumn("y").getInitialAutoIncrementValue());
        Assert.assertEquals(null, table.getColumn("z").getInitialAutoIncrementValue());

        Assert.assertEquals(1, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

/*

    @Test
    public void testDDLGen()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", "customer_pk", true, "primary key");
        builder.indexColumn("schema", "customer", "customer_pk", "customer_id", 0, true, null);
        builder.index("schema", "customer", "customer_name", false, "key");
        builder.indexColumn("schema", "customer", "customer_name", "customer_name", 0, true, null);
        builder.userTable("schema", "order");
        builder.column("schema", "order", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "customer_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L, false, false, null, null);
        builder.userTable("schema", "item");
        builder.column("schema", "item", "item_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "order_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "quantity", 2, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "item_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        DDLGenerator ddlGenerator = new DDLGenerator();
        print(ddlGenerator.dropAllGroupTables(ais));
        print(ddlGenerator.createAllGroupTables(ais));

        builder.akibanInformationSchema().checkIntegrity();
    }
*/

    @Test
    public void testCycles()
    {
        AISBuilder builder = new AISBuilder();
        // q(k)
        builder.userTable("s", "q");
        builder.column("s", "q", "k", 0, "int", 0L, 0L, false, false, null, null);
        builder.index("s", "q", "q_pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "q", "q_pk", "k", 0, true, null);
        // p(k, qk -> q(k))
        builder.userTable("s", "p");
        builder.column("s", "p", "k", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "p", "qk", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("s", "p", "p_pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "p", "p_pk", "k", 0, true, null);
        builder.joinTables("pq", "s", "q", "s", "p");
        builder.joinColumns("pq", "s", "q", "k", "s", "p", "qk");
        // t(k, p -> p(k), fk -> t(k))
        builder.userTable("s", "t");
        builder.column("s", "t", "k", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "t", "p", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "t", "fk", 2, "int", 0L, 0L, false, false, null, null);
        builder.index("s", "t", "t_pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "t", "t_pk", "k", 0, true, null);
        builder.joinTables("tt", "s", "t", "s", "t");
        builder.joinColumns("tt", "s", "t", "k", "s", "t", "fk");
        builder.joinTables("tp", "s", "p", "s", "t");
        builder.joinColumns("tp", "s", "p", "k", "s", "t", "p");
        builder.basicSchemaIsComplete();
        // group: p->q, t->p, t->t
        builder.createGroup("group", "s", "g");
        builder.addTableToGroup("group", "s", "q");
        builder.addJoinToGroup("group", "pq", 0);
        builder.addTableToGroup("group", "s", "p");
        builder.addJoinToGroup("group", "tp", 0);
        builder.addTableToGroup("group", "s", "t");
        try {
            builder.addJoinToGroup("group", "tt", 0);
            Assert.fail();
        } catch (AISBuilder.GroupStructureException e) {
            // expected
        }
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Table userTable = ais.getTable("s", "t");
        userTable.getColumns();
        Table groupTable = ais.getTable("s", "g");
        groupTable.getColumns();

        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS);
        
        Assert.assertEquals(1, vResults.failures().size());
        AISValidationFailure fail = vResults.failures().iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_MULTIPLE_PARENTS, fail.errorCode());
    }

    @Test
    public void testFunnyFKs()
    {
        AISBuilder builder = new AISBuilder();
        // parent table
        builder.userTable("s", "parent");
        // parent columns
        builder.column("s", "parent", "pk", 0, "int", 0L, 0L, false, false, null, null); // , null, nullPK
        builder.column("s", "parent", "uk", 1, "int", 0L, 0L, false, false, null, null); // unique k, null, nulley
        builder.column("s", "parent", "nk", 2, "int", 0L, 0L, false, false, null, null); // non-k, null, nulley
        // parent indexes
        builder.index("s", "parent", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "parent", "pk", "pk", 0, true, null);
        builder.index("s", "parent", "uk", true, "UNIQUE KEY");
        builder.indexColumn("s", "parent", "uk", "uk", 0, true, null);
        builder.index("s", "parent", "nk", true, "KEY");
        builder.indexColumn("s", "parent", "nk", "nk", 0, true, null);
        // child table
        builder.userTable("s", "child");
        // child columns
        builder.column("s", "child", "ck", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "child", "fk_pk", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "child", "fk_uk", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("s", "child", "fk_nk", 3, "int", 0L, 0L, false, false, null, null);
        // joins
        builder.joinTables("pkjoin", "s", "parent", "s", "child");
        builder.joinColumns("pkjoin", "s", "parent", "pk", "s", "child", "fk_pk");
        builder.joinTables("ukjoin", "s", "parent", "s", "child");
        builder.joinColumns("ukjoin", "s", "parent", "uk", "s", "child", "fk_uk");
        builder.joinTables("nkjoin", "s", "parent", "s", "child");
        builder.joinColumns("nkjoin", "s", "parent", "nk", "s", "child", "fk_nk");
        // Create group
        builder.basicSchemaIsComplete();
        builder.createGroup("g", "s", "g");
        // Add pk join to group
        builder.addTableToGroup("g", "s", "parent");
        builder.addJoinToGroup("g", "pkjoin", 0);
/* Grouping validation has been disabled, so these tests will fail.
        // Add uk join to group
        builder.removeJoinFromGroup("g", "pkjoin");
        try {
            builder.addJoinToGroup("g", "ukjoin", 0);
            Assert.fail();
        } catch (AISBuilder.UngroupableJoinException e) {
            // expected
        }
        // Add nk join to group
        try {
            builder.addJoinToGroup("g", "nkjoin", 0);
            Assert.fail();
        } catch (Exception e) {
            // expected
        }
*/
        // Done
        builder.groupingIsComplete();
        
        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS);
        
        Assert.assertEquals(4, vResults.failures().size());
        Iterator<AISValidationFailure> fails = vResults.failures().iterator();
        // Failure 1: join to unique key
        AISValidationFailure fail = fails.next();
        Assert.assertEquals(ErrorCode.JOIN_TO_MULTIPLE_PARENTS, fail.errorCode());
        // Failure 2: join to non-key
        fail = fails.next();
        Assert.assertEquals(ErrorCode.JOIN_TO_MULTIPLE_PARENTS, fail.errorCode());
        // Failure 3: 3 joins to parent
        fail = fails.next();
        Assert.assertEquals(ErrorCode.JOIN_TO_WRONG_COLUMNS, fail.errorCode());
        Assert.assertEquals("Table `s`.`child` join reference part `nk` does not match `s`.`parent` primary key part `pk`", fail.message());
        // Failure 4: 3 joins to parent
        fail = fails.next();
        Assert.assertEquals(ErrorCode.JOIN_TO_WRONG_COLUMNS, fail.errorCode());
        Assert.assertEquals("Table `s`.`child` join reference part `uk` does not match `s`.`parent` primary key part `pk`", fail.message());
    }

    @Test
    public void testIndexedLength()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", "idx_customer_name", false, Index.KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", "idx_customer_name", "customer_name", 0, true, null);
        builder.index("schema", "customer", "idx_customer_name_partial", false, Index.KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", "idx_customer_name_partial", "customer_name", 0, true, 5);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        UserTable table = ais.getUserTable("schema", "customer");
        Assert.assertNull(table.getIndex("idx_customer_name").getKeyColumns().get(0).getIndexedLength());
        Assert.assertEquals(5, table.getIndex("idx_customer_name_partial").getKeyColumns().get(0).getIndexedLength().intValue());
    }

    @Test
    public void testAISCharsetAndCollation()
    {
        AISBuilder builder = new AISBuilder();
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        CharsetAndCollation charsetAndCollation = ais.getCharsetAndCollation();
        Assert.assertNotNull(charsetAndCollation);
        Assert.assertEquals(AkibanInformationSchema.getDefaultCharset(), charsetAndCollation.charset());
        Assert.assertEquals(AkibanInformationSchema.getDefaultCollation(), charsetAndCollation.collation());
    }

    @Test
    public void testUserTableCharsetAndCollation()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        UserTable table = ais.getUserTable("schema", "customer");
        CharsetAndCollation charsetAndCollation = table.getCharsetAndCollation();
        Assert.assertNotNull(charsetAndCollation);
        Assert.assertEquals(AkibanInformationSchema.getDefaultCharset(), charsetAndCollation.charset());
        Assert.assertEquals(AkibanInformationSchema.getDefaultCollation(), charsetAndCollation.collation());
    }

    @Test
    public void testGroupTableCharsetAndCollation()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "schema", "customer_group");
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        builder.addTableToGroup("group", "schema", "customer");
        builder.groupingIsComplete();
        GroupTable table = ais.getGroupTable("schema", "customer_group");
        CharsetAndCollation charsetAndCollation = table.getCharsetAndCollation();
        Assert.assertNotNull(charsetAndCollation);
        Assert.assertEquals(AkibanInformationSchema.getDefaultCharset(), charsetAndCollation.charset());
        Assert.assertEquals(AkibanInformationSchema.getDefaultCollation(), charsetAndCollation.collation());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testUserColumnDefaultCharsetAndCollation()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_name", 0, "varchar", 100L, 0L, false, false, null, null);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        UserTable table = ais.getUserTable("schema", "customer");
        Column column = table.getColumn("customer_name");
        CharsetAndCollation charsetAndCollation = column.getCharsetAndCollation();
        Assert.assertNotNull(charsetAndCollation);
        Assert.assertEquals(AkibanInformationSchema.getDefaultCharset(), charsetAndCollation.charset());
        Assert.assertEquals(AkibanInformationSchema.getDefaultCollation(), charsetAndCollation.collation());
    }

    @Test
    public void testGroupColumnDefaultCharsetAndCollation()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_name", 0, "varchar", 100L, 0L, false, false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "schema", "customer_group");
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        builder.addTableToGroup("group", "schema", "customer");
        builder.groupingIsComplete();
        GroupTable table = ais.getGroupTable("schema", "customer_group");
        Column column = table.getColumn(0);
        CharsetAndCollation charsetAndCollation = column.getCharsetAndCollation();
        Assert.assertNotNull(charsetAndCollation);
        Assert.assertEquals(AkibanInformationSchema.getDefaultCharset(), charsetAndCollation.charset());
        Assert.assertEquals(AkibanInformationSchema.getDefaultCollation(), charsetAndCollation.collation());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testCharsetAndCollationOverride()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_name", 0, "varchar", 100L, 0L, false, false, "UTF16", "latin1_swedish_ci");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "schema", "customer_group");
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        builder.addTableToGroup("group", "schema", "customer");
        builder.groupingIsComplete();
        UserTable userTable = ais.getUserTable("schema", "customer");
        Column userColumn = userTable.getColumn(0);
        CharsetAndCollation charsetAndCollation = userColumn.getCharsetAndCollation();
        Assert.assertNotNull(charsetAndCollation);
        Assert.assertEquals("UTF16", charsetAndCollation.charset());
        Assert.assertEquals("latin1_swedish_ci", charsetAndCollation.collation());
        GroupTable groupTable = ais.getGroupTable("schema", "customer_group");
        Column groupColumn = groupTable.getColumn(0);
        charsetAndCollation = groupColumn.getCharsetAndCollation();
        Assert.assertNotNull(charsetAndCollation);
        Assert.assertEquals("UTF16", charsetAndCollation.charset());
        Assert.assertEquals("latin1_swedish_ci", charsetAndCollation.collation());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
    }

    @Test
    public void testTwoTableGroupWithGroupIndex()
    {
        final AISBuilder builder = new AISBuilder();
        builder.userTable("test", "c");
        builder.column("test", "c", "id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "c", "name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("test", "c", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("test", "c", Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);
        builder.userTable("test", "o");
        builder.column("test", "o", "oid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "o", "cid", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "o", "date", 2, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("c/id/o/cid", "test", "c", "test", "o");
        builder.joinColumns("c/id/o/cid", "test", "c", "id", "test", "o", "cid");
        builder.basicSchemaIsComplete();
        builder.createGroup("coi", "test", "_akiban_c");
        builder.addJoinToGroup("coi", "c/id/o/cid", 0);
        builder.groupIndex("coi", "name_date", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("coi", "name_date", "test", "c",  "name", 0);
        builder.groupIndexColumn("coi", "name_date", "test", "o",  "date", 1);
        builder.groupingIsComplete();

        builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).throwIfNecessary();
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
        
        final AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(2, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());

        final Group group = ais.getGroup("coi");
        Assert.assertEquals(1, group.getIndexes().size());
        final Index index = group.getIndex("name_date");
        Assert.assertNotNull(index);
        Assert.assertEquals(2, index.getKeyColumns().size());

        GroupIndex groupIndex = (GroupIndex) index;
        Assert.assertEquals("group indexes for c", 1, ais.getUserTable("test", "c").getGroupIndexes().size());
        Assert.assertTrue("GI for c missing its group index",
                ais.getUserTable("test", "c").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group indexes for o", 1, ais.getUserTable("test", "o").getGroupIndexes().size());
        Assert.assertTrue("GI for o missing its group index",
                ais.getUserTable("test", "o").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group index rootmost", ais.getUserTable("test", "c"), groupIndex.rootMostTable());
        Assert.assertEquals("group index leafmost", ais.getUserTable("test", "o"), groupIndex.leafMostTable());
    }

    @Test
    public void testLeapfroggingGroupIndex()
    {
        final AISBuilder builder = new AISBuilder();
        builder.userTable("test", "c");
        builder.column("test", "c", "id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "c", "name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("test", "c", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("test", "c", "pk", "id", 0, true, null);
        
        builder.userTable("test", "o");
        builder.column("test", "o", "oid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "o", "cid", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "o", "date", 2, "int", 0L, 0L, false, false, null, null);
        builder.index("test", "o", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("test", "o", "pk", "oid", 0, true, null);
        builder.joinTables("c/id/o/cid", "test", "c", "test", "o");
        builder.joinColumns("c/id/o/cid", "test", "c", "id", "test", "o", "cid");

        builder.userTable("test", "i");
        builder.column("test", "i", "iid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "i", "oid", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "i", "sku", 2, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("o/oid/i/iid", "test", "o", "test", "i");
        builder.joinColumns("o/oid/i/iid", "test", "o", "oid", "test", "i", "iid");

        builder.basicSchemaIsComplete();
        builder.createGroup("coi", "test", "_akiban_c");
        builder.addJoinToGroup("coi", "c/id/o/cid", 0);
        builder.addJoinToGroup("coi", "o/oid/i/iid", 0);
        builder.groupIndex("coi", "name_sku", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("coi", "name_sku", "test", "c",  "name", 0);
        builder.groupIndexColumn("coi", "name_sku", "test", "i",  "sku", 1);
        builder.groupingIsComplete();

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
        
        final AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(3, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());

        final Group group = ais.getGroup("coi");
        Assert.assertEquals(1, group.getIndexes().size());
        final Index index = group.getIndex("name_sku");
        Assert.assertNotNull(index);
        Assert.assertEquals(2, index.getKeyColumns().size());

        GroupIndex groupIndex = (GroupIndex) index;
        Assert.assertEquals("group indexes for c", 1, ais.getUserTable("test", "c").getGroupIndexes().size());
        Assert.assertTrue("GI for c missing its group index",
                ais.getUserTable("test", "c").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group indexes for o", 0, ais.getUserTable("test", "o").getGroupIndexes().size());
        Assert.assertFalse("GI for o has its group index",
                ais.getUserTable("test", "o").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group indexes for i", 1, ais.getUserTable("test", "i").getGroupIndexes().size());
        Assert.assertTrue("GI for i missing its group index",
                ais.getUserTable("test", "i").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group index rootmost", ais.getUserTable("test", "c"), groupIndex.rootMostTable());
        Assert.assertEquals("group index leafmost", ais.getUserTable("test", "i"), groupIndex.leafMostTable());
    }

    @Test
    public void testGroupIndexBuiltOutOfOrder()
    {
        final AISBuilder builder = new AISBuilder();
        builder.userTable("test", "c");
        builder.column("test", "c", "id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "c", "name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("test", "c", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("test", "c", "pk", "id", 0, true, null);

        builder.userTable("test", "o");
        builder.column("test", "o", "oid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "o", "cid", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "o", "date", 2, "int", 0L, 0L, false, false, null, null);
        builder.index("test", "o", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("test", "o", "pk", "oid", 0, true, null);
        
        builder.joinTables("c/id/o/cid", "test", "c", "test", "o");
        builder.joinColumns("c/id/o/cid", "test", "c", "id", "test", "o", "cid");

        builder.userTable("test", "i");
        builder.column("test", "i", "iid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "i", "oid", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("test", "i", "sku", 2, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("o/oid/i/iid", "test", "o", "test", "i");
        builder.joinColumns("o/oid/i/iid", "test", "o", "oid", "test", "i", "iid");

        builder.basicSchemaIsComplete();
        builder.createGroup("coi", "test", "_akiban_c");
        builder.addJoinToGroup("coi", "c/id/o/cid", 0);
        builder.addJoinToGroup("coi", "o/oid/i/iid", 0);
        builder.groupIndex("coi", "name_date_sku", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("coi", "name_date_sku", "test", "c",  "name", 0);
        builder.groupIndexColumn("coi", "name_date_sku", "test", "i",  "sku", 1);
        builder.groupIndexColumn("coi", "name_date_sku", "test", "o",  "date", 1);
        builder.groupingIsComplete();

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS).failures().size());
        
        final AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(3, ais.getUserTables().size());
        Assert.assertEquals(1, ais.getGroupTables().size());
        Assert.assertEquals(1, ais.getGroups().size());

        final Group group = ais.getGroup("coi");
        Assert.assertEquals(1, group.getIndexes().size());
        final Index index = group.getIndex("name_date_sku");
        Assert.assertNotNull(index);
        Assert.assertEquals(3, index.getKeyColumns().size());

        GroupIndex groupIndex = (GroupIndex) index;
        Assert.assertEquals("group indexes for c", 1, ais.getUserTable("test", "c").getGroupIndexes().size());
        Assert.assertTrue("GI for c missing its group index",
                ais.getUserTable("test", "c").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group indexes for o", 1, ais.getUserTable("test", "o").getGroupIndexes().size());
        Assert.assertTrue("GI for o missing its group index",
                ais.getUserTable("test", "o").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group indexes for i", 1, ais.getUserTable("test", "i").getGroupIndexes().size());
        Assert.assertTrue("GI for i missing its group index",
                ais.getUserTable("test", "i").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group index rootmost", ais.getUserTable("test", "c"), groupIndex.rootMostTable());
        Assert.assertEquals("group index leafmost", ais.getUserTable("test", "i"), groupIndex.leafMostTable());
    }

    @Test(expected = IllegalArgumentException.class)
    public void groupIndexOnUngroupedTable()
    {
        final AISBuilder builder = new AISBuilder();
        try {
            builder.userTable("test", "c");
            builder.column("test", "c", "id", 0, "int", 0L, 0L, false, false, null, null);
            builder.column("test", "c", "name", 1, "varchar", 64L, 0L, false, false, null, null);
            builder.basicSchemaIsComplete();
            builder.createGroup("coi", "test", "_akiban_c");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        builder.groupIndex("coi", "name_date", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("coi", "name_date", "test", "c",  "name", 0);
    }

    @Test(expected = AISBuilder.NoSuchObjectException.class)
    public void groupIndexMultiGroup()
    {
        final AISBuilder builder = new AISBuilder();
        try {
            builder.userTable("test", "c");
            builder.column("test", "c", "id", 0, "int", 0L, 0L, false, false, null, null);
            builder.column("test", "c", "name", 1, "varchar", 64L, 0L, false, false, null, null);
            builder.userTable("test", "o");
            builder.column("test", "o", "oid", 0, "int", 0L, 0L, false, false, null, null);
            builder.column("test", "o", "cid", 1, "int", 0L, 0L, false, false, null, null);
            builder.column("test", "o", "date", 2, "int", 0L, 0L, false, false, null, null);
            builder.basicSchemaIsComplete();
            builder.createGroup("coi1", "test", "_akiban_c");
            builder.createGroup("coi2", "test", "_akiban_o");
            builder.addTableToGroup("coi1", "test", "c");
            builder.addTableToGroup("coi2", "test", "o");
            builder.groupIndex("coi1", "name_date", false, Index.JoinType.LEFT);
            builder.groupIndexColumn("coi1", "name_date", "test", "c",  "name", 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        builder.groupIndexColumn("coi2", "name_date", "test", "o",  "date", 1);
    }

    @Test(expected = BranchingGroupIndexException.class)
    public void groupIndexMultiBranch()
    {
        final AISBuilder builder = new AISBuilder();
        try {
            builder.userTable("test", "c");
            builder.column("test", "c", "id", 0, "int", 0L, 0L, false, false, null, null);
            builder.column("test", "c", "name", 1, "varchar", 64L, 0L, false, false, null, null);

            builder.userTable("test", "o");
            builder.column("test", "o", "oid", 0, "int", 0L, 0L, false, false, null, null);
            builder.column("test", "o", "cid", 1, "int", 0L, 0L, false, false, null, null);
            builder.column("test", "o", "date", 2, "int", 0L, 0L, false, false, null, null);
            builder.joinTables("c/id/o/cid", "test", "c", "test", "o");
            builder.joinColumns("c/id/o/cid", "test", "c", "id", "test", "o", "cid");

            builder.userTable("test", "a");
            builder.column("test", "a", "oid", 0, "int", 0L, 0L, false, false, null, null);
            builder.column("test", "a", "cid", 1, "int", 0L, 0L, false, false, null, null);
            builder.column("test", "a", "address", 2, "int", 0L, 0L, false, false, null, null);
            builder.joinTables("c/id/a/cid", "test", "c", "test", "a");
            builder.joinColumns("c/id/a/cid", "test", "c", "id", "test", "a", "cid");

            builder.basicSchemaIsComplete();
            builder.createGroup("coi", "test", "_akiban_c");
            builder.addJoinToGroup("coi", "c/id/o/cid", 0);
            builder.addJoinToGroup("coi", "c/id/a/cid", 0);
            builder.groupIndex("coi", "name_date", false, Index.JoinType.LEFT);
            builder.groupIndexColumn("coi", "name_date", "test", "o",  "date", 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        builder.groupIndexColumn("coi", "name_date", "test", "a",  "address", 1);
    }
    
    @Test
    public void setIdentityValues() {
        final AISBuilder builder = new AISBuilder();
        builder.userTable("test", "t1");
        builder.column("test", "t1", "id", 0, "int", 0L, 0L, false, false, null, null);
        builder.sequence("test", "seq-1", 1, 1, 0, 1000, false);
        builder.columnAsIdentity("test", "t1", "id", "seq-1", true);
        builder.column("test", "t1", "name", 1, "varchar", 10L, 0L, false, false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "test", "coi");
        builder.addTableToGroup("group", "test", "t1");
        builder.groupingIsComplete();
        
        UserTable table = builder.akibanInformationSchema().getUserTable("test", "t1");
        Column column = table.getColumn(0);
        assertNotNull (column.getDefaultIdentity());
        assertNotNull (column.getIdentityGenerator());
        //assertEquals (table.getTreeName(), column.getIdentityGenerator().getTreeName());
        //assertEquals (new Integer(3), column.getIdentityGenerator().getAccumIndex());
        
        
    }
}
