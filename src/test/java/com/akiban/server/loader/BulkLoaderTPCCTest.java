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

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;

// Check task details for bulk load of TPCC schema -- just part of the customer group: customer-order-order_line.

public class BulkLoaderTPCCTest extends TestCase
{
    @Test
    public void testTPCC() throws ClassNotFoundException, SQLException
    {
        final AkibanInformationSchema ais = ais();
        checkTasks(ais, bulkLoad(ais));
    }

    private AkibanInformationSchema ais()
    {
        AISBuilder builder = new AISBuilder();
        // customer(w, d, c, cx) pk: (w, d, c)
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "w", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "d", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "c", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "cx", 3, "varchar", 100L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", "pk", "w", 0, true, 0);
        builder.indexColumn("schema", "customer", "pk", "d", 1, true, 0);
        builder.indexColumn("schema", "customer", "pk", "c", 2, true, 0);
        // order(w, d, o, c, ox) pk: (w, d, o) fk: (w, d, c)
        builder.userTable("schema", "order");
        builder.column("schema", "order", "w", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "d", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "o", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "c", 3, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "ox", 4, "varchar", 50L, 0L, false, false, null, null);
        builder.index("schema", "order", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "order", "pk", "w", 0, true, 0);
        builder.indexColumn("schema", "order", "pk", "d", 1, true, 0);
        builder.indexColumn("schema", "order", "pk", "o", 2, true, 0);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "w", "schema", "order", "w");
        builder.joinColumns("co", "schema", "customer", "d", "schema", "order", "d");
        builder.joinColumns("co", "schema", "customer", "c", "schema", "order", "c");
        // order_line(w, d, o, n, olx) pk: (w, d, o, n) fk: (w, d, o)
        builder.userTable("schema", "order_line");
        builder.column("schema", "order_line", "w", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order_line", "d", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order_line", "o", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order_line", "n", 3, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order_line", "olx", 4, "double", 0L, 0L, false, false, null, null);
        builder.index("schema", "order_line", "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "order_line", "pk", "w", 0, true, 0);
        builder.indexColumn("schema", "order_line", "pk", "d", 1, true, 0);
        builder.indexColumn("schema", "order_line", "pk", "o", 2, true, 0);
        builder.indexColumn("schema", "order_line", "pk", "n", 3, true, 0);
        builder.joinTables("ool", "schema", "order", "schema", "order_line");
        builder.joinColumns("ool", "schema", "order", "w", "schema", "order_line", "w");
        builder.joinColumns("ool", "schema", "order", "d", "schema", "order_line", "d");
        builder.joinColumns("ool", "schema", "order", "o", "schema", "order_line", "o");
        builder.basicSchemaIsComplete();
        // Create group
        builder.createGroup("coi", "coi", "coi");
        builder.addJoinToGroup("coi", "co", 0);
        builder.addJoinToGroup("coi", "ool", 0);
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
        UserTable orderLine = ais.getUserTable("schema", "order_line");
        checkCustomer(customer, tasks.get(customer));
        checkOrder(order, tasks.get(order));
        checkOrderLine(orderLine, tasks.get(orderLine));
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
        checkColumns(finalTask.hKey(), "customer.w", "customer.d", "customer.c");
        checkColumns(finalTask.columns(), "customer.w", "customer.d", "customer.c", "customer.cx");
        checkColumns(finalTask.order(), "customer.w", "customer.d", "customer.c");
    }

    private void checkOrder(UserTable order, TableTasks orderTasks)
    {
        // Order contains its own hkey, so it's final task is a sort. Also need parent task
        // for computation of child (order_line) final task.
        Assert.assertNull(orderTasks.generateChild());
        // Check final task
        GenerateFinalTask finalTask = orderTasks.generateFinal();
        Assert.assertNotNull(finalTask);
        Assert.assertTrue(finalTask instanceof GenerateFinalBySortTask);
        Assert.assertSame(order, finalTask.table());
        checkColumns(finalTask.hKey(), "order.w", "order.d", "order.c", "order.o");
        checkColumns(finalTask.columns(), "order.w", "order.d", "order.o", "order.c", "order.ox");
        checkColumns(finalTask.order(), "order.w", "order.d", "order.c", "order.o");
        // Check parent task
        GenerateParentTask parentTask = orderTasks.generateParent();
        Assert.assertNotNull(parentTask);
        Assert.assertTrue(parentTask instanceof GenerateParentBySortTask);
        Assert.assertSame(order, parentTask.table());
        checkColumns(parentTask.hKey(), "order.w", "order.d", "order.c", "order.o");
        checkColumns(parentTask.columns(), "order.w", "order.d", "order.c", "order.o");
        checkColumns(parentTask.order(), "order.w", "order.d", "order.o");
    }

    private void checkOrderLine(UserTable orderLine, TableTasks orderLineTasks)
    {
        // orderLine needs all three tasks. 1) orderLine$child merges with order$parent to compute orderLine$parent.
        // orderLine$parent merges with orderLine to form orderLine$final
        // Check child task
        GenerateChildTask childTask = orderLineTasks.generateChild();
        Assert.assertNotNull(childTask);
        Assert.assertSame(orderLine, childTask.table());
        checkColumns(childTask.hKey(), "order_line.w", "order_line.d", "order.c", "order_line.o", "order_line.n");
        checkColumns(childTask.columns(), "order_line.w", "order_line.d", "order_line.o", "order_line.n");
        checkColumns(childTask.order(), "order_line.w", "order_line.d", "order_line.o");
        // Check parent task
        GenerateParentTask parentTask = orderLineTasks.generateParent();
        Assert.assertNotNull(parentTask);
        Assert.assertTrue(parentTask instanceof GenerateParentByMergeTask);
        checkColumns(parentTask.hKey(), "order_line.w", "order_line.d", "order.c", "order_line.o", "order_line.n");
        checkColumns(parentTask.columns(), "order_line.w", "order_line.d", "order_line.o", "order_line.n", "order.c");
        checkColumns(parentTask.order(), "order_line.w", "order_line.d", "order_line.o");
        // Check final task
        GenerateFinalTask finalTask = orderLineTasks.generateFinal();
        Assert.assertNotNull(finalTask);
        Assert.assertTrue(finalTask instanceof GenerateFinalByMergeTask);
        checkColumns(finalTask.hKey(), "order_line.w", "order_line.d", "order.c", "order_line.o", "order_line.n");
        checkColumns(finalTask.columns(), "order_line.w", "order_line.d", "order_line.o", "order_line.n", "order_line.olx", "order.c");
        checkColumns(finalTask.order(), "order_line.w", "order_line.d", "order.c", "order_line.o", "order_line.n");
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
