/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.ais.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationResults;
import com.foundationdb.ais.model.validation.AISValidations;
import com.foundationdb.server.error.BranchingGroupIndexException;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;

public class AISBuilderTest
{
    private final TypesRegistry typesRegistry = TestTypesRegistry.MCOMPAT;

    private TInstance type(String bundleName, String typeName, boolean nullable) {
        return type(bundleName, typeName, null, null, nullable);
    }

    private TInstance type(String bundleName, String typeName,
                           Long typeParameter1, Long typeParameter2,
                           boolean nullable) {
        return type(bundleName, typeName, typeParameter1, typeParameter2, null, null, nullable);
    }

    private TInstance type(String bundleName, String typeName,
                           Long typeParameter1, Long typeParameter2,
                           String charset, String collation,
                           boolean nullable) {
        return typesRegistry.getType(bundleName, typeName, typeParameter1, typeParameter2, charset, collation, nullable,
                                     null, null, null);
    }

    @Test
    public void testEmptyAIS()
    {
        AISBuilder builder = new AISBuilder();
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(0, ais.getTables().size());
        Assert.assertEquals(0, ais.getGroups().size());
        Assert.assertEquals(0, ais.getJoins().size());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }
    
    @Test
    public void testSingleTableNoGroups()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(1, ais.getTables().size());
        Assert.assertEquals(0, ais.getGroups().size());
        Assert.assertEquals(0, ais.getJoins().size());

        Assert.assertEquals(1, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
       
    }

    @Test
    public void testSingleTableInGroup()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addTableToGroup("group", "schema", "customer");
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(1, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(0, ais.getJoins().size());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
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
        builder.table("schema", "customer");
        builder.column("schema", tableName, columnOne, 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", tableName, columnTwo, 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addTableToGroup("group", "schema", "customer");
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(1, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(0, ais.getJoins().size());

        Assert.assertEquals(0,
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testSingleJoinInGroup()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "customer_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "order_date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addJoinToGroup("group", "co", 0);
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(2, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(1, ais.getJoins().size());
        Assert.assertEquals(0,
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testTableAndThenSingleJoinInGroup()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "customer_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "order_date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addTableToGroup("group", "schema", "customer");
        builder.addJoinToGroup("group", "co", 0);
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(2, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(1, ais.getJoins().size());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testTwoJoinsInGroup()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", false), false, null, null);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "customer_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "order_date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("schema", "order");
        builder.indexColumn("schema", "order", Index.PRIMARY, "order_id", 0, true, null);
        builder.table("schema", "item");
        builder.column("schema", "item", "item_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "item", "order_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "item", "quantity", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "item_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(3, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(2, ais.getJoins().size());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testTwoJoinsInGroupThenClearAndRetry()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "customer_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "order_date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("schema", "order");
        builder.indexColumn("schema", "order", Index.PRIMARY, "order_id", 0, true, null);
        builder.table("schema", "item");
        builder.column("schema", "item", "item_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "item", "order_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "item", "quantity", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "item_id");
        builder.basicSchemaIsComplete();

        // Step 1 -- group
        builder.createGroup("group", "groupschema");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        {
            AkibanInformationSchema ais = builder.akibanInformationSchema();
            Assert.assertEquals(3, ais.getTables().size());
            Assert.assertEquals(1, ais.getGroups().size());
            Assert.assertEquals(2, ais.getJoins().size());
        }

        // Step 2 -- clear
        builder.clearGroupings();
        {
            AkibanInformationSchema ais = builder.akibanInformationSchema();
            Assert.assertEquals(3, ais.getTables().size());
            Assert.assertEquals(0, ais.getGroups().size());
            Assert.assertEquals(2, ais.getJoins().size());
            Assert.assertNull( ais.getGroup( new TableName("schema", "group")) );
        }

        // Step 3 -- regroup with different name
        builder.createGroup("group2", "groupschema");
        builder.addJoinToGroup("group2", "co", 0);
        builder.addJoinToGroup("group2", "oi", 0);
        {
            builder.groupingIsComplete();
            AkibanInformationSchema ais = builder.akibanInformationSchema();
            Assert.assertEquals(3, ais.getTables().size());
            Assert.assertEquals(1, ais.getGroups().size());
            Assert.assertEquals(2, ais.getJoins().size());
        }
        
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testRemoval()
    {
        // Setup as in testTwoJoinsInGroup
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "customer_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "order_date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("schema", "order");
        builder.indexColumn("schema", "order", Index.PRIMARY, "order_id", 0, true, null);
        builder.table("schema", "item");
        builder.column("schema", "item", "item_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "item", "order_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "item", "quantity", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "item_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        builder.groupingIsComplete();

        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(3, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(2, ais.getJoins().size());
        // Remove customer/order join
        builder.removeJoinFromGroup("group", "co");
        Assert.assertEquals(3, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(2, ais.getJoins().size());
        // Remove order/item join
        builder.removeJoinFromGroup("group", "oi");
        Assert.assertEquals(3, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(2, ais.getJoins().size());

        AISValidationResults results = builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
        
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
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "customer_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "order_date", 2, type("MCOMPAT", "int", false), false, null, null);
        // Create join from order to customer
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        // Create customer
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addTableToGroup("group", "schema", "customer");
        builder.addJoinToGroup("group", "co", 0);
        builder.groupingIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(2, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());
        Assert.assertEquals(1, ais.getJoins().size());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testDeleteGroupWithOneTable()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addTableToGroup("group", "schema", "customer");
        builder.removeTableFromGroup("group", "schema", "customer");
        builder.deleteGroup("group");
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(1, ais.getTables().size());
        Assert.assertEquals(0, ais.getGroups().size());
        Assert.assertEquals(0, ais.getJoins().size());

        Assert.assertEquals(1, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testDeleteGroupWithOneJoin()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "customer_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "order", "order_date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("schema", "order");
        builder.indexColumn("schema", "order", Index.PRIMARY, "order_id", 0, true, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addTableToGroup("group", "schema", "customer");
        builder.addJoinToGroup("group", "co", 0);
        builder.removeJoinFromGroup("group", "co");
        builder.deleteGroup("group");
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(2, ais.getTables().size());
        Assert.assertEquals(0, ais.getGroups().size());
        Assert.assertEquals(1, ais.getJoins().size());

        Assert.assertEquals(2, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testMoveTreeToEmptyGroup()
    {
        AISBuilder builder = new AISBuilder();
        // Source group tables: a(b(c, d))
        builder.table("s", "a");
        builder.column("s", "a", "aid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("s", "a");
        builder.indexColumn("s", "a", Index.PRIMARY, "aid", 0, true, null);
        builder.table("s", "b");
        builder.column("s", "b", "bid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "b", "aid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("s", "b");
        builder.indexColumn("s", "b", Index.PRIMARY, "bid", 0, true, null);
        builder.table("s", "c");
        builder.column("s", "c", "cid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "c", "bid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.table("s", "d");
        builder.column("s", "d", "did", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "d", "bid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("ab", "s", "a", "s", "b");
        builder.joinColumns("ab", "s", "a", "aid", "s", "b", "aid");
        builder.joinTables("bc", "s", "b", "s", "c");
        builder.joinColumns("bc", "s", "b", "bid", "s", "c", "bid");
        builder.joinTables("bd", "s", "b", "s", "d");
        builder.joinColumns("bd", "s", "b", "bid", "s", "d", "bid");
        // Source and target groups
        builder.basicSchemaIsComplete();
        builder.createGroup("source", "g");
        builder.addJoinToGroup("source", "ab", 0);
        builder.addJoinToGroup("source", "bc", 0);
        builder.addJoinToGroup("source", "bd", 0);
        builder.createGroup("target", "g");

        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(4, ais.getTables().size());
        Assert.assertEquals(2, ais.getGroups().size());
        Assert.assertEquals(3, ais.getJoins().size());
        // Move b to target
        builder.moveTreeToEmptyGroup("s", "b", "target");
        builder.groupingIsComplete();

        Table a = ais.getTable("s", "a");
        List<Join> aChildren = a.getChildJoins();
        Assert.assertTrue(aChildren.isEmpty());
        Table b = ais.getTable("s", "b");
        for (Join join : b.getChildJoins()) {
            if (join.getChild() == ais.getTable("s", "c") ||
                join.getChild() == ais.getTable("s", "d")) {
            } else {
                Assert.fail();
            }
        }

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testMoveTreeToEmptyGroup_bug95()
    {
        AISBuilder builder = new AISBuilder();

        builder.table("s", "c");
        builder.column("s", "c", "c_id", 0, type("MCOMPAT", "int", false), true, null, null);
        builder.pk("s", "c");
        builder.indexColumn("s", "c", Index.PRIMARY, "c_id", 0, true, null);

        builder.table("s", "o");
        builder.column("s", "o", "o_id", 0, type("MCOMPAT", "int", false), true, null, null);
        builder.column("s", "o", "c_id", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("s", "o");
        builder.indexColumn("s", "o", Index.PRIMARY, "o_id", 0, true, null);
        builder.index("s", "o", "customer");
        builder.indexColumn("s", "o", "customer", "c_id", 0, false, null);
        builder.basicSchemaIsComplete();

        builder.joinTables("co", "s", "c", "s", "o");
        builder.joinColumns("co", "s", "c", "c_id", "s", "o", "c_id");

        builder.createGroup("group_01", "s");
        builder.addTableToGroup("group_01", "s", "c");
        builder.addJoinToGroup("group_01", "co", 1);
        builder.groupingIsComplete();

        Assert.assertEquals(0,
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());

        builder.createGroup("group_02", "s");
        builder.moveTreeToEmptyGroup("s", "o", "group_02");
        builder.groupingIsComplete();

        Assert.assertEquals(0,
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());

        builder.groupingIsComplete();
        Assert.assertEquals(0,
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testMoveTreeToNonEmptyGroup() throws Exception {
        AISBuilder builder = new AISBuilder();
        // Source group tables: a(b(c, d))
        builder.table("s", "a");
        builder.column("s", "a", "aid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("s", "a");
        builder.indexColumn("s", "a", Index.PRIMARY, "aid", 0, true, null);
        builder.table("s", "b");
        builder.column("s", "b", "bid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "b", "aid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("s", "b");
        builder.indexColumn("s", "b", Index.PRIMARY, "bid", 0, true, null);
        builder.table("s", "c");
        builder.column("s", "c", "cid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "c", "bid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.table("s", "d");
        builder.column("s", "d", "did", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "d", "bid", 1, type("MCOMPAT", "int", false), false, null, null);
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
        builder.table("s", "z");
        builder.column("s", "z", "zid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("s", "z");
        builder.indexColumn("s", "z", Index.PRIMARY, "zid", 0, true, null);
        // Source and target groups
        builder.basicSchemaIsComplete();
        builder.createGroup("source", "g");
        builder.addJoinToGroup("source", "ab", 0);
        builder.addJoinToGroup("source", "bc", 0);
        builder.addJoinToGroup("source", "bd", 0);
        builder.createGroup("target", "g");
        builder.addTableToGroup("target", "s", "z");
        builder.groupingIsComplete();

        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(5, ais.getTables().size());
        Assert.assertEquals(2, ais.getGroups().size());
        Assert.assertEquals(4, ais.getJoins().size());
        // Move b to target
        builder.moveTreeToGroup("s", "b", "target", "bz");
        Table a = ais.getTable("s", "a");
        List<Join> aChildren = a.getChildJoins();
        Assert.assertTrue(aChildren.isEmpty());
        Table z = ais.getTable("s", "z");
        Assert.assertEquals(1, z.getChildJoins().size());
        Join bz = z.getChildJoins().get(0);
        Table b = ais.getTable("s", "b");
        Assert.assertSame(b, bz.getChild());
        for (Join join : b.getChildJoins()) {
            if (join.getChild() == ais.getTable("s", "c") ||
                join.getChild() == ais.getTable("s", "d")) {
            } else {
                Assert.fail();
            }
        }
        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
        
        Assert.assertEquals(1, vResults.failures().size());
        AISValidationFailure fail = vResults.failures().iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_MULTIPLE_PARENTS, fail.errorCode());
    }

    @Test
    public void testInitialAutoInc()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("s", "b");
        builder.column("s", "b", "x", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "b", "y", 1, type("MCOMPAT", "int", false), true, null, null);
        builder.column("s", "b", "z", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.tableInitialAutoIncrement("s", "b", 5L);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        // Check autoinc state
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Table table = ais.getTable("s", "b");
        Assert.assertEquals(3, table.getColumns().size());
        Assert.assertEquals(table.getColumn("y"), table.getAutoIncrementColumn());
        Assert.assertEquals(null, table.getColumn("x").getInitialAutoIncrementValue());
        Assert.assertEquals(5L, table.getColumn("y").getInitialAutoIncrementValue().longValue());
        Assert.assertEquals(null, table.getColumn("z").getInitialAutoIncrementValue());

        Assert.assertEquals(1, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testInitialAutoIncNoAutoInc()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("s", "b");
        builder.column("s", "b", "x", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "b", "y", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "b", "z", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.tableInitialAutoIncrement("s", "b", 5L);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        // Check autoinc state
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Table table = ais.getTable("s", "b");
        Assert.assertEquals(3, table.getColumns().size());
        Assert.assertEquals(null, table.getAutoIncrementColumn());
        Assert.assertEquals(null, table.getColumn("x").getInitialAutoIncrementValue());
        Assert.assertEquals(null, table.getColumn("y").getInitialAutoIncrementValue());
        Assert.assertEquals(null, table.getColumn("z").getInitialAutoIncrementValue());

        Assert.assertEquals(1, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testCycles()
    {
        AISBuilder builder = new AISBuilder();
        // q(k)
        builder.table("s", "q");
        builder.column("s", "q", "k", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("s", "q");
        builder.indexColumn("s", "q", Index.PRIMARY, "k", 0, true, null);
        // p(k, qk -> q(k))
        builder.table("s", "p");
        builder.column("s", "p", "k", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "p", "qk", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("s", "p");
        builder.indexColumn("s", "p", Index.PRIMARY, "k", 0, true, null);
        builder.joinTables("pq", "s", "q", "s", "p");
        builder.joinColumns("pq", "s", "q", "k", "s", "p", "qk");
        // t(k, p -> p(k), fk -> t(k))
        builder.table("s", "t");
        builder.column("s", "t", "k", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "t", "p", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "t", "fk", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("s", "t");
        builder.indexColumn("s", "t", Index.PRIMARY, "k", 0, true, null);
        builder.joinTables("tt", "s", "t", "s", "t");
        builder.joinColumns("tt", "s", "t", "k", "s", "t", "fk");
        builder.joinTables("tp", "s", "p", "s", "t");
        builder.joinColumns("tp", "s", "p", "k", "s", "t", "p");
        builder.basicSchemaIsComplete();
        // group: p->q, t->p, t->t
        builder.createGroup("group", "s");
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
        Table table = ais.getTable("s", "t");
        table.getColumns();

        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
        
        Assert.assertEquals(1, vResults.failures().size());
        AISValidationFailure fail = vResults.failures().iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_MULTIPLE_PARENTS, fail.errorCode());
    }

    @Test
    public void testFunnyFKs()
    {
        AISBuilder builder = new AISBuilder();
        // parent table
        builder.table("s", "parent");
        // parent columns
        builder.column("s", "parent", "pk", 0, type("MCOMPAT", "int", false), false, null, null); // , null, nullPK
        builder.column("s", "parent", "uk", 1, type("MCOMPAT", "int", false), false, null, null); // unique k, null, nulley
        builder.column("s", "parent", "nk", 2, type("MCOMPAT", "int", false), false, null, null); // non-k, null, nulley
        // parent indexes
        builder.pk("s", "parent");
        builder.indexColumn("s", "parent", Index.PRIMARY, "pk", 0, true, null);
        builder.unique("s", "parent", "uk");
        builder.indexColumn("s", "parent", "uk", "uk", 0, true, null);
        builder.unique("s", "parent", "nk");
        builder.indexColumn("s", "parent", "nk", "nk", 0, true, null);
        // child table
        builder.table("s", "child");
        // child columns
        builder.column("s", "child", "ck", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "child", "fk_pk", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "child", "fk_uk", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.column("s", "child", "fk_nk", 3, type("MCOMPAT", "int", false), false, null, null);
        // joins
        builder.joinTables("pkjoin", "s", "parent", "s", "child");
        builder.joinColumns("pkjoin", "s", "parent", "pk", "s", "child", "fk_pk");
        builder.joinTables("ukjoin", "s", "parent", "s", "child");
        builder.joinColumns("ukjoin", "s", "parent", "uk", "s", "child", "fk_uk");
        builder.joinTables("nkjoin", "s", "parent", "s", "child");
        builder.joinColumns("nkjoin", "s", "parent", "nk", "s", "child", "fk_nk");
        // Create group
        builder.basicSchemaIsComplete();
        builder.createGroup("g", "s");
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
        
        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
        
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
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("schema", "customer", "customer_name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.index("schema", "customer", "idx_customer_name");
        builder.indexColumn("schema", "customer", "idx_customer_name", "customer_name", 0, true, null);
        builder.index("schema", "customer", "idx_customer_name_partial");
        builder.indexColumn("schema", "customer", "idx_customer_name_partial", "customer_name", 0, true, 5);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Table table = ais.getTable("schema", "customer");
        Assert.assertNull(table.getIndex("idx_customer_name").getKeyColumns().get(0).getIndexedLength());
        Assert.assertEquals(5, table.getIndex("idx_customer_name_partial").getKeyColumns().get(0).getIndexedLength().intValue());
    }

    @Test
    public void testAISCharsetAndCollation()
    {
        AISBuilder builder = new AISBuilder();
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(AkibanInformationSchema.getDefaultCharsetId(), ais.getCharsetId());
        Assert.assertEquals(AkibanInformationSchema.getDefaultCollationId(), ais.getCollationId());
    }

    @Test
    public void testTableCharsetAndCollation()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Table table = ais.getTable("schema", "customer");
        Assert.assertEquals(AkibanInformationSchema.getDefaultCharsetId(), table.getDefaultedCharsetId());
        Assert.assertEquals(AkibanInformationSchema.getDefaultCollationId(), table.getDefaultedCollationId());
    }

    @Test
    public void testUserColumnDefaultCharsetAndCollation()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_name", 0, type("MCOMPAT", "varchar", 100L, null, false), false, null, null);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        Table table = ais.getTable("schema", "customer");
        Column column = table.getColumn("customer_name");
        Assert.assertEquals(AkibanInformationSchema.getDefaultCharsetId(), column.getCharsetId());
        Assert.assertEquals(AkibanInformationSchema.getDefaultCollationId(), column.getCollationId());
    }

    @Test
    public void testGroupColumnDefaultCharsetAndCollation()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_name", 0, type("MCOMPAT", "varchar", 100L, null, false), false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "schema");
        builder.addTableToGroup("group", "schema", "customer");
        builder.groupingIsComplete();

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testCharsetAndCollationOverride()
    {
        AISBuilder builder = new AISBuilder();
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_name", 0, type("MCOMPAT", "varchar", 100L, null, "UTF16", "latin1_swedish_ci", false), false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "schema");
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        builder.addTableToGroup("group", "schema", "customer");
        builder.groupingIsComplete();
        Table table = ais.getTable("schema", "customer");
        Column userColumn = table.getColumn(0);
        Assert.assertEquals("UTF16", userColumn.getCharsetName());
        Assert.assertEquals("latin1_swedish_ci", userColumn.getCollationName());

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
    }

    @Test
    public void testTwoTableGroupWithGroupIndex()
    {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "c");
        builder.column("test", "c", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "c", "name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("test", "c");
        builder.indexColumn("test", "c", Index.PRIMARY, "id", 0, true, null);
        builder.table("test", "o");
        builder.column("test", "o", "oid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "o", "cid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "o", "date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("c/id/o/cid", "test", "c", "test", "o");
        builder.joinColumns("c/id/o/cid", "test", "c", "id", "test", "o", "cid");
        builder.basicSchemaIsComplete();
        builder.createGroup("coi", "test");
        builder.addJoinToGroup("coi", "c/id/o/cid", 0);
        builder.groupIndex("coi", "name_date", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("coi", "name_date", "test", "c",  "name", 0);
        builder.groupIndexColumn("coi", "name_date", "test", "o",  "date", 1);
        builder.groupingIsComplete();

        builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).throwIfNecessary();
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
        
        final AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(2, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());

        final Group group = ais.getGroup(new TableName("test", "coi"));
        Assert.assertEquals(1, group.getIndexes().size());
        final Index index = group.getIndex("name_date");
        Assert.assertNotNull(index);
        Assert.assertEquals(2, index.getKeyColumns().size());

        GroupIndex groupIndex = (GroupIndex) index;
        Assert.assertEquals("group indexes for c", 1, ais.getTable("test", "c").getGroupIndexes().size());
        Assert.assertTrue("GI for c missing its group index",
                ais.getTable("test", "c").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group indexes for o", 1, ais.getTable("test", "o").getGroupIndexes().size());
        Assert.assertTrue("GI for o missing its group index",
                ais.getTable("test", "o").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group index rootmost", ais.getTable("test", "c"), groupIndex.rootMostTable());
        Assert.assertEquals("group index leafmost", ais.getTable("test", "o"), groupIndex.leafMostTable());
    }

    @Test
    public void testLeapfroggingGroupIndex()
    {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "c");
        builder.column("test", "c", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "c", "name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("test", "c");
        builder.indexColumn("test", "c", Index.PRIMARY, "id", 0, true, null);
        
        builder.table("test", "o");
        builder.column("test", "o", "oid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "o", "cid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "o", "date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("test", "o");
        builder.indexColumn("test", "o", Index.PRIMARY, "oid", 0, true, null);
        builder.joinTables("c/id/o/cid", "test", "c", "test", "o");
        builder.joinColumns("c/id/o/cid", "test", "c", "id", "test", "o", "cid");

        builder.table("test", "i");
        builder.column("test", "i", "iid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "i", "oid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "i", "sku", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("o/oid/i/iid", "test", "o", "test", "i");
        builder.joinColumns("o/oid/i/iid", "test", "o", "oid", "test", "i", "iid");

        builder.basicSchemaIsComplete();
        builder.createGroup("coi", "test");
        builder.addJoinToGroup("coi", "c/id/o/cid", 0);
        builder.addJoinToGroup("coi", "o/oid/i/iid", 0);
        builder.groupIndex("coi", "name_sku", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("coi", "name_sku", "test", "c",  "name", 0);
        builder.groupIndexColumn("coi", "name_sku", "test", "i",  "sku", 1);
        builder.groupingIsComplete();

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
        
        final AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(3, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());

        final Group group = ais.getGroup( new TableName("test","coi"));
        Assert.assertEquals(1, group.getIndexes().size());
        final Index index = group.getIndex("name_sku");
        Assert.assertNotNull(index);
        Assert.assertEquals(2, index.getKeyColumns().size());

        GroupIndex groupIndex = (GroupIndex) index;
        Assert.assertEquals("group indexes for c", 1, ais.getTable("test", "c").getGroupIndexes().size());
        Assert.assertTrue("GI for c missing its group index",
                ais.getTable("test", "c").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group indexes for o", 1, ais.getTable("test", "o").getGroupIndexes().size());
        Assert.assertTrue("GI for o has its group index",
                ais.getTable("test", "o").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group indexes for i", 1, ais.getTable("test", "i").getGroupIndexes().size());
        Assert.assertTrue("GI for i missing its group index",
                ais.getTable("test", "i").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group index rootmost", ais.getTable("test", "c"), groupIndex.rootMostTable());
        Assert.assertEquals("group index leafmost", ais.getTable("test", "i"), groupIndex.leafMostTable());
    }

    @Test
    public void testGroupIndexBuiltOutOfOrder()
    {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "c");
        builder.column("test", "c", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "c", "name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("test", "c");
        builder.indexColumn("test", "c", Index.PRIMARY, "id", 0, true, null);

        builder.table("test", "o");
        builder.column("test", "o", "oid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "o", "cid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "o", "date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.pk("test", "o");
        builder.indexColumn("test", "o", Index.PRIMARY, "oid", 0, true, null);
        
        builder.joinTables("c/id/o/cid", "test", "c", "test", "o");
        builder.joinColumns("c/id/o/cid", "test", "c", "id", "test", "o", "cid");

        builder.table("test", "i");
        builder.column("test", "i", "iid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "i", "oid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "i", "sku", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("o/oid/i/iid", "test", "o", "test", "i");
        builder.joinColumns("o/oid/i/iid", "test", "o", "oid", "test", "i", "iid");

        builder.basicSchemaIsComplete();
        builder.createGroup("coi", "test");
        builder.addJoinToGroup("coi", "c/id/o/cid", 0);
        builder.addJoinToGroup("coi", "o/oid/i/iid", 0);
        builder.groupIndex("coi", "name_date_sku", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("coi", "name_date_sku", "test", "c",  "name", 0);
        builder.groupIndexColumn("coi", "name_date_sku", "test", "i",  "sku", 1);
        builder.groupIndexColumn("coi", "name_date_sku", "test", "o",  "date", 1);
        builder.groupingIsComplete();

        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
        
        final AkibanInformationSchema ais = builder.akibanInformationSchema();
        Assert.assertEquals(3, ais.getTables().size());
        Assert.assertEquals(1, ais.getGroups().size());

        final Group group = ais.getGroup(new TableName("test","coi"));
        Assert.assertEquals(1, group.getIndexes().size());
        final Index index = group.getIndex("name_date_sku");
        Assert.assertNotNull(index);
        Assert.assertEquals(3, index.getKeyColumns().size());

        GroupIndex groupIndex = (GroupIndex) index;
        Assert.assertEquals("group indexes for c", 1, ais.getTable("test", "c").getGroupIndexes().size());
        Assert.assertTrue("GI for c missing its group index",
                ais.getTable("test", "c").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group indexes for o", 1, ais.getTable("test", "o").getGroupIndexes().size());
        Assert.assertTrue("GI for o missing its group index",
                ais.getTable("test", "o").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group indexes for i", 1, ais.getTable("test", "i").getGroupIndexes().size());
        Assert.assertTrue("GI for i missing its group index",
                ais.getTable("test", "i").getGroupIndexes().contains(groupIndex)
        );

        Assert.assertEquals("group index rootmost", ais.getTable("test", "c"), groupIndex.rootMostTable());
        Assert.assertEquals("group index leafmost", ais.getTable("test", "i"), groupIndex.leafMostTable());
    }

    @Test(expected = IllegalArgumentException.class)
    public void groupIndexOnTableNotInGroup()
    {
        final AISBuilder builder = new AISBuilder();
        try {
            builder.table("test", "c");
            builder.column("test", "c", "id", 0, type("MCOMPAT", "int", false), false, null, null);
            builder.column("test", "c", "name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
            builder.table("test", "o");
            builder.basicSchemaIsComplete();
            builder.createGroup("c", "test");
            builder.addTableToGroup("c", "test", "c");
            builder.createGroup("oi", "test");
            builder.addTableToGroup("oi", "test", "o");
            builder.groupingIsComplete();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        builder.groupIndex("oi", "name_date", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("oi", "name_date", "test", "c",  "name", 0);
    }

    @Test(expected = AISBuilder.NoSuchObjectException.class)
    public void groupIndexMultiGroup()
    {
        final AISBuilder builder = new AISBuilder();
        try {
            builder.table("test", "c");
            builder.column("test", "c", "id", 0, type("MCOMPAT", "int", false), false, null, null);
            builder.column("test", "c", "name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
            builder.table("test", "o");
            builder.column("test", "o", "oid", 0, type("MCOMPAT", "int", false), false, null, null);
            builder.column("test", "o", "cid", 1, type("MCOMPAT", "int", false), false, null, null);
            builder.column("test", "o", "date", 2, type("MCOMPAT", "int", false), false, null, null);
            builder.basicSchemaIsComplete();
            builder.createGroup("coi1", "test");
            builder.createGroup("coi2", "test");
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
            builder.table("test", "c");
            builder.column("test", "c", "id", 0, type("MCOMPAT", "int", false), false, null, null);
            builder.column("test", "c", "name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);

            builder.table("test", "o");
            builder.column("test", "o", "oid", 0, type("MCOMPAT", "int", false), false, null, null);
            builder.column("test", "o", "cid", 1, type("MCOMPAT", "int", false), false, null, null);
            builder.column("test", "o", "date", 2, type("MCOMPAT", "int", false), false, null, null);
            builder.joinTables("c/id/o/cid", "test", "c", "test", "o");
            builder.joinColumns("c/id/o/cid", "test", "c", "id", "test", "o", "cid");

            builder.table("test", "a");
            builder.column("test", "a", "oid", 0, type("MCOMPAT", "int", false), false, null, null);
            builder.column("test", "a", "cid", 1, type("MCOMPAT", "int", false), false, null, null);
            builder.column("test", "a", "address", 2, type("MCOMPAT", "int", false), false, null, null);
            builder.joinTables("c/id/a/cid", "test", "c", "test", "a");
            builder.joinColumns("c/id/a/cid", "test", "c", "id", "test", "a", "cid");

            builder.basicSchemaIsComplete();
            builder.createGroup("coi", "test");
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
        builder.table("test", "t1");
        builder.column("test", "t1", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.sequence("test", "seq-1", 1, 1, 0, 1000, false);
        builder.columnAsIdentity("test", "t1", "id", "seq-1", true);
        builder.column("test", "t1", "name", 1, type("MCOMPAT", "varchar", 10L, null, false), false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "test");
        builder.addTableToGroup("group", "test", "t1");
        builder.groupingIsComplete();
        
        Table table = builder.akibanInformationSchema().getTable("test", "t1");
        Column column = table.getColumn(0);
        assertNotNull (column.getDefaultIdentity());
        assertNotNull (column.getIdentityGenerator());
    }

    @Test
    public void validateIdentityNoPKFails() {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "t1");
       
        builder.column("test", "t1", "ident", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.sequence("test", "seq-1", 1, 1, 0, 1000, false);
        builder.columnAsIdentity("test", "t1", "ident", "seq-1", true);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "test");
        builder.addTableToGroup("group", "test", "t1");
        builder.groupingIsComplete();
        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
        Assert.assertEquals(1, vResults.failures().size());
        AISValidationFailure fail = vResults.failures().iterator().next();
        Assert.assertEquals(ErrorCode.MULTIPLE_IDENTITY_COLUMNS, fail.errorCode());
    }

    
    @Test
    public void validateIdentityGoodValues() {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.index("test", "t1", Index.PRIMARY, true, true, TableName.create("test", Index.PRIMARY));
        builder.indexColumn("test", "t1", Index.PRIMARY, "id", 0, true, null);
        
        builder.column("test", "t1", "ident", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.sequence("test", "seq-1", 1, 1, 0, 1000, false);
        builder.columnAsIdentity("test", "t1", "ident", "seq-1", true);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "test");
        builder.addTableToGroup("group", "test", "t1");
        builder.groupingIsComplete();
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).failures().size());
        
    }
    
    @Test
    public void validateIdentityZeroIncrement() {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.index("test", "t1", Index.PRIMARY, true, true, TableName.create("test", Index.PRIMARY));
        builder.indexColumn("test", "t1", Index.PRIMARY, "id", 0, true, null);
        
        builder.column("test", "t1", "ident", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.sequence("test", "seq-1", 1, 0, 0, 1000, false);
        builder.columnAsIdentity("test", "t1", "ident", "seq-1", true);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "test");
        builder.addTableToGroup("group", "test", "t1");
        builder.groupingIsComplete();
        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
        
        Assert.assertEquals(1, vResults.failures().size());
        AISValidationFailure fail = vResults.failures().iterator().next();
        Assert.assertEquals(ErrorCode.SEQUENCE_INTERVAL_ZERO, fail.errorCode());
    }

    @Test
    public void validateIdentityMinMax1() {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.index("test", "t1", Index.PRIMARY, true, true, TableName.create("test", Index.PRIMARY));
        builder.indexColumn("test", "t1", Index.PRIMARY, "id", 0, true, null);

        builder.column("test", "t1", "ident", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.sequence("test", "seq-1", 1, 1, 1000, 0, false);
        builder.columnAsIdentity("test", "t1", "ident", "seq-1", true);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "test");
        builder.addTableToGroup("group", "test", "t1");
        builder.groupingIsComplete();
        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
        
        Assert.assertEquals(2, vResults.failures().size());
        Iterator<AISValidationFailure> errors = vResults.failures().iterator();
        
        AISValidationFailure fail = errors.next();
        assertEquals(ErrorCode.SEQUENCE_MIN_GE_MAX, fail.errorCode());
        fail = errors.next();
        assertEquals(ErrorCode.SEQUENCE_START_IN_RANGE, fail.errorCode());
    }

    @Test
    public void validateIdentityMinMax2() {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.index("test", "t1", Index.PRIMARY, true, true, TableName.create("test", Index.PRIMARY));
        builder.indexColumn("test", "t1", Index.PRIMARY, "id", 0, true, null);
        builder.column("test", "t1", "ident", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.sequence("test", "seq-1", 1000, 1, 1000, 1000, false);
        builder.columnAsIdentity("test", "t1", "ident", "seq-1", true);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "test");
        builder.addTableToGroup("group", "test", "t1");
        builder.groupingIsComplete();
        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
        
        Assert.assertEquals(1, vResults.failures().size());
        AISValidationFailure fail = vResults.failures().iterator().next();
        Assert.assertEquals(ErrorCode.SEQUENCE_MIN_GE_MAX, fail.errorCode());
    }

    @Test
    public void validateIdentityVarchar() {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.index("test", "t1", Index.PRIMARY, true, true, TableName.create("test", Index.PRIMARY));
        builder.indexColumn("test", "t1", Index.PRIMARY, "id", 0, true, null);
        builder.column("test", "t1", "ident", 1, type("MCOMPAT", "varchar", 32L, null, false), false, null, null);
        builder.sequence("test", "seq-1", 1, 1, 1, 1000, false);
        builder.columnAsIdentity("test", "t1", "ident", "seq-1", true);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "test");
        builder.addTableToGroup("group", "test", "t1");
        builder.groupingIsComplete();
        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
        Assert.assertEquals(1, vResults.failures().size());
        AISValidationFailure fail = vResults.failures().iterator().next();
        assertEquals(ErrorCode.GENERATOR_WRONG_DATATYPE, fail.errorCode());
    }
    
    @Test
    public void validateIdentityDecimal() {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.index("test", "t1", Index.PRIMARY, true, true, TableName.create("test", Index.PRIMARY));
        builder.indexColumn("test", "t1", Index.PRIMARY, "id", 0, true, null);
        builder.column("test", "t1", "ident", 1, type("MCOMPAT", "decimal", false), false, null, null);
        builder.sequence("test", "seq-1", 1, 1, 1, 1000, false);
        builder.columnAsIdentity("test", "t1", "ident", "seq-1", true);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "test");
        builder.addTableToGroup("group", "test", "t1");
        builder.groupingIsComplete();
        AISValidationResults vResults = builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
        Assert.assertEquals(1, vResults.failures().size());
        AISValidationFailure fail = vResults.failures().iterator().next();
        assertEquals(ErrorCode.GENERATOR_WRONG_DATATYPE, fail.errorCode());
    }
    
    
    private AISBuilder twoChildGroup () {
        final AISBuilder builder = new AISBuilder();
        builder.table("test", "c");
        builder.column("test", "c", "id", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "c", "name", 1, type("MCOMPAT", "varchar", 64L, null, false), false, null, null);
        builder.pk("test", "c");
        builder.indexColumn("test", "c", Index.PRIMARY, "id", 0, true, null);
        builder.table("test", "o");
        builder.column("test", "o", "oid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "o", "cid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "o", "date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("c/id/o/cid", "test", "c", "test", "o");
        builder.joinColumns("c/id/o/cid", "test", "c", "id", "test", "o", "cid");
        builder.basicSchemaIsComplete();
        TableName groupName = TableName.create("test", "coi");
        builder.createGroup(groupName.getTableName(), groupName.getSchemaName());
        builder.addJoinToGroup(groupName, "c/id/o/cid", 1);
        builder.addTableToGroup(groupName, "test", "c");
        builder.addTableToGroup(groupName, "test", "o");
        
        return builder;
        
    }
    
    @Test
    public void validateNameForOutputSimple() {
        AISBuilder builder = twoChildGroup();
        
        builder.groupingIsComplete();
        Table c = builder.akibanInformationSchema().getTable("test", "c");
        Table o = builder.akibanInformationSchema().getTable("test", "o");
        
        Assert.assertEquals("test.c", c.getNameForOutput());
        Assert.assertEquals("o", o.getNameForOutput());
    }
    
    @Test
    public void validateNameForOutputCase1() {
        AISBuilder builder = twoChildGroup();
        builder.column("test", "c", "o", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.groupingIsComplete();
        Table c = builder.akibanInformationSchema().getTable("test", "c");
        Table o = builder.akibanInformationSchema().getTable("test", "o");
        
        Assert.assertEquals("test.c", c.getNameForOutput());
        Assert.assertEquals("_o", o.getNameForOutput());
    }
    
    @Test
    public void validateNameForOutputCase2() {
        AISBuilder builder = twoChildGroup();
        builder.table("test", "_o");
        builder.column("test", "_o", "oid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "_o", "cid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "_o", "date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("c/id/_o/cid", "test", "c", "test", "_o");
        builder.joinColumns("c/id/_o/cid", "test", "c", "id", "test", "_o", "cid");
        TableName groupName = TableName.create("test", "coi");
        builder.addJoinToGroup(groupName, "c/id/_o/cid", 1);
        builder.addTableToGroup(groupName, "test", "_o");
        
        builder.groupingIsComplete();
        Table o = builder.akibanInformationSchema().getTable("test", "o");
        Table _o = builder.akibanInformationSchema().getTable("test", "_o");
        
        Assert.assertEquals("o", o.getNameForOutput());
        Assert.assertEquals("_o", _o.getNameForOutput());
    }
    
    @Test
    public void validateNameForOutputCase3() {
        AISBuilder builder = twoChildGroup();
        builder.column("test", "c", "o", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.table("test", "_o");
        builder.column("test", "_o", "oid", 0, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "_o", "cid", 1, type("MCOMPAT", "int", false), false, null, null);
        builder.column("test", "_o", "date", 2, type("MCOMPAT", "int", false), false, null, null);
        builder.joinTables("c/id/_o/cid", "test", "c", "test", "_o");
        builder.joinColumns("c/id/_o/cid", "test", "c", "id", "test", "_o", "cid");
        TableName groupName = TableName.create("test", "coi");
        builder.addJoinToGroup(groupName, "c/id/_o/cid", 1);
        builder.addTableToGroup(groupName, "test", "_o");
        
        builder.groupingIsComplete();
        Table o = builder.akibanInformationSchema().getTable("test", "o");
        Table _o = builder.akibanInformationSchema().getTable("test", "_o");
        
        Assert.assertEquals("__o", o.getNameForOutput());
        Assert.assertEquals("_o", _o.getNameForOutput());
    }
    
}
