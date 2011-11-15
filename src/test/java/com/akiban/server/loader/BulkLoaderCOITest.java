/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.loader;

import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;
import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;

// Check task details for bulk load of COI schema

public class BulkLoaderCOITest extends TestCase
{
    @Test
    public void testCOI() throws ClassNotFoundException, SQLException
    {
        final AkibanInformationSchema ais = ais();
        checkTasks(ais, bulkLoad(ais));
    }

    private AkibanInformationSchema ais()
    {
        AISBuilder builder = new AISBuilder();
        // customer(cid) pk: cid
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "cid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "c_name", 1, "varchar", 100L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", "pk", "cid", 0, true, 0);
        // order(oid, cid) pk: oid, fk: cid
        builder.userTable("schema", "order");
        builder.column("schema", "order", "oid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "cid", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "o_date", 2, "varchar", 50L, 0L, false, false, null, null);
        builder.index("schema", "order", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "order", "pk", "oid", 0, true, 0);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "cid", "schema", "order", "cid");
        // item(iid, oid) pk: iid, fk: oid
        builder.userTable("schema", "item");
        builder.column("schema", "item", "iid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "oid", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "quantity", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "unit_price", 3, "double", 0L, 0L, false, false, null, null);
        builder.index("schema", "item", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "item", "pk", "iid", 0, true, 0);
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "oid", "schema", "item", "oid");
        builder.basicSchemaIsComplete();
        // Create group
        builder.createGroup("coi", "coi", "coi");
        builder.addJoinToGroup("coi", "co", 0);
        builder.addJoinToGroup("coi", "oi", 0);
        builder.groupingIsComplete();
        return builder.akibanInformationSchema();
    }

    private IdentityHashMap<UserTable, TableTasks> bulkLoad(AkibanInformationSchema ais)
        throws ClassNotFoundException, SQLException
    {
        TaskGenerator.Actions actions = new MySQLTaskGeneratorActions(ais);
        BulkLoader bulkLoader = new BulkLoader(null, ais, "coi", "bulkload", actions);
        bulkLoader.run();
        return bulkLoader.tasks();
    }

    private void checkTasks(AkibanInformationSchema ais, IdentityHashMap<UserTable, TableTasks> tasks)
    {
        UserTable customer = ais.getUserTable("schema", "customer");
        UserTable order = ais.getUserTable("schema", "order");
        UserTable item = ais.getUserTable("schema", "item");
        checkCustomer(customer, tasks.get(customer));
        checkOrder(order, tasks.get(order));
        checkItem(item, tasks.get(item));
    }

    private void checkCustomer(UserTable customer, TableTasks customerTasks)
    {
        // Customer is root, and child contains its own hkey. So all that's needed is a sort.
        Assert.assertNull(customerTasks.generateChild());
        Assert.assertNull(customerTasks.generateParent());
        GenerateFinalTask finalTask = customerTasks.generateFinal();
        Assert.assertNotNull(finalTask);
        Assert.assertTrue(finalTask instanceof GenerateFinalBySortTask);
        Assert.assertSame(customer, finalTask.table());
        checkColumns(finalTask.hKey(), "customer.cid");
        checkColumns(finalTask.columns(), "customer.cid", "customer.c_name");
        checkColumns(finalTask.order(), "customer.cid");
    }

    private void checkOrder(UserTable order, TableTasks orderTasks)
    {
        // Order contains its own hkey, so it's final task is a sort. Also need parent task
        // for computation of child (item) final task.
        Assert.assertNull(orderTasks.generateChild());
        // Check final task
        GenerateFinalTask finalTask = orderTasks.generateFinal();
        Assert.assertNotNull(finalTask);
        Assert.assertTrue(finalTask instanceof GenerateFinalBySortTask);
        Assert.assertSame(order, finalTask.table());
        checkColumns(finalTask.hKey(), "order.cid", "order.oid");
        checkColumns(finalTask.columns(), "order.oid", "order.cid", "order.o_date");
        checkColumns(finalTask.order(), "order.cid", "order.oid");
        // Check parent task
        GenerateParentTask parentTask = orderTasks.generateParent();
        Assert.assertNotNull(parentTask);
        Assert.assertTrue(parentTask instanceof GenerateParentBySortTask);
        Assert.assertSame(order, parentTask.table());
        checkColumns(parentTask.hKey(), "order.cid", "order.oid");
        checkColumns(parentTask.columns(), "order.cid", "order.oid");
        checkColumns(parentTask.order(), "order.oid");
    }

    private void checkItem(UserTable item, TableTasks itemTasks)
    {
        // Item needs all three tasks. 1) item$child merges with order$parent to compute item$parent.
        // item$parent merges with item to form item$final
        // Check child task
        GenerateChildTask childTask = itemTasks.generateChild();
        Assert.assertNotNull(childTask);
        Assert.assertSame(item, childTask.table());
        checkColumns(childTask.hKey(), "order.cid", "item.oid", "item.iid");
        checkColumns(childTask.columns(), "item.oid", "item.iid");
        checkColumns(childTask.order(), "item.oid");
        // Check parent task
        GenerateParentTask parentTask = itemTasks.generateParent();
        Assert.assertNotNull(parentTask);
        Assert.assertTrue(parentTask instanceof GenerateParentByMergeTask);
        checkColumns(parentTask.hKey(), "order.cid", "item.oid", "item.iid");
        checkColumns(parentTask.columns(), "item.oid", "item.iid", "order.cid");
        checkColumns(parentTask.order(), "item.oid");
        // Check final task
        GenerateFinalTask finalTask = itemTasks.generateFinal();
        Assert.assertNotNull(finalTask);
        Assert.assertTrue(finalTask instanceof GenerateFinalByMergeTask);
        checkColumns(finalTask.hKey(), "order.cid", "item.oid", "item.iid");
        checkColumns(finalTask.columns(), "item.iid", "item.oid", "item.quantity", "item.unit_price", "order.cid");
        checkColumns(finalTask.order(), "order.cid", "item.oid", "item.iid");
    }

    private void checkColumns(List<Column> actual, String... expectedColumnNames)
    {
        Assert.assertEquals(expectedColumnNames.length, actual.size());
        int i = 0;
        for (Column column : actual) {
            String actualColumnName =
                String.format("%s.%s", column.getTable().getName().getTableName(), column.getName());
            Assert.assertEquals(expectedColumnNames[i++], actualColumnName);
        }
    }
}
