/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.qp.row;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.foundationdb.server.test.ApiTestBase;
import org.junit.Test;

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.rowtype.FlattenedRowType;
import com.foundationdb.qp.rowtype.ProductRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.UserTableRowType;
import com.foundationdb.server.rowdata.SchemaFactory;

public class CompoundRowTest {

    
    @Test
    public void testFlattenRow() {
        Schema schema = caoiSchema();
        
        UserTable customer = schema.ais().getUserTable("schema", "customer");
        UserTable order = schema.ais().getUserTable("schema", "order");
        
        RowType customerType = schema.userTableRowType(customer);
        RowType orderType = schema.userTableRowType(order);
        
        ValuesRow customerRow = new ValuesRow (customerType, new Integer(1), new String ("fred"));
        ValuesRow orderRow = new ValuesRow (orderType, new Integer (1000), new Integer(1), new Integer(45));

        FlattenedRowType flattenType = schema.newFlattenType(customerType, orderType);
        
        FlattenedRow flattenRow = new FlattenedRow(flattenType, customerRow, orderRow, orderRow.hKey());
        
        assertTrue(flattenRow.containsRealRowOf(customer));
        assertTrue (flattenRow.containsRealRowOf(order));
        // Can't test this because ValuesRow throws UnsupportedOperationException for this check.
        //assertFalse(flattenRow.containsRealRowOf(state));
        
        assertEquals(ApiTestBase.getLong(flattenRow, 0), Long.valueOf(1));
        assertEquals(flattenRow.eval(1).getString(), "fred");
        assertEquals(ApiTestBase.getLong(flattenRow, 2), Long.valueOf(1000));
        assertEquals(ApiTestBase.getLong(flattenRow, 3), Long.valueOf(1));
        assertEquals(ApiTestBase.getLong(flattenRow, 4), Long.valueOf(45));
    }
    
    @Test
    public void testProductRow() {
        Schema schema = caoiSchema();
        
        UserTable customer = schema.ais().getUserTable("schema", "customer");
        UserTable order = schema.ais().getUserTable("schema", "order");
        
        RowType customerType = schema.userTableRowType(customer);
        RowType orderType = schema.userTableRowType(order);
        
        ValuesRow customerRow = new ValuesRow (customerType, new Integer(1), new String("Fred"));
        ValuesRow ordersRow = new ValuesRow (orderType, new Integer(1000), new Integer(1), new Integer(45));
        
        ProductRowType productType = schema.newProductType(customerType, (UserTableRowType)customerType, orderType);
        
        ProductRow productRow = new ProductRow (productType, customerRow, ordersRow);
        
        assertNotNull (productRow);
       
        assertEquals(ApiTestBase.getLong(productRow, 0), Long.valueOf(1));
        assertEquals(productRow.eval(1).getString(), "Fred");
        assertEquals(ApiTestBase.getLong(productRow, 2), Long.valueOf(45));
        
    }

    private Schema caoiSchema() {
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
        builder.index("schema", "item", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "item", Index.PRIMARY_KEY_CONSTRAINT, "item_id", 0, true, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "item_id");
        builder.userTable("schema", "state");
        builder.column("schema", "state", "code", 0, "varchar", 2L, 0L, false, false, null, null);
        builder.column("schema", "state", "name", 1, "varchar", 50L, 0L, false, false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        builder.createGroup("state", "schema");
        builder.addTableToGroup("state", "schema", "state");
        builder.groupingIsComplete();
        
        SchemaFactory factory = new SchemaFactory ("schema");
        factory.buildRowDefs(builder.akibanInformationSchema());
        return new Schema(builder.akibanInformationSchema());
    }
    
}
