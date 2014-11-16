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

package com.foundationdb.qp.row;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.foundationdb.qp.rowtype.*;
import com.foundationdb.server.test.ApiTestBase;
import org.junit.Test;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.server.types.service.TestTypesRegistry;

public class CompoundRowTest {

    @Test
    public void testFlattenRow() {
        Schema schema = caoiSchema();
        
        Table customer = schema.ais().getTable("schema", "customer");
        Table order = schema.ais().getTable("schema", "order");
        
        RowType customerType = schema.tableRowType(customer);
        RowType orderType = schema.tableRowType(order);
        
        ValuesHolderRow customerRow = new ValuesHolderRow(customerType, new Integer(1), new String ("fred"));
        ValuesHolderRow orderRow = new ValuesHolderRow(orderType, new Integer (1000), new Integer(1), new Integer(45));

        FlattenedRowType flattenType = schema.newFlattenType(customerType, orderType);
        
        FlattenedRow flattenRow = new FlattenedRow(flattenType, customerRow, orderRow, null);
        
        assertTrue(flattenRow.containsRealRowOf(customer));
        assertTrue (flattenRow.containsRealRowOf(order));
        // Can't test this because ValuesHolderRow throws UnsupportedOperationException for this check.
        //assertFalse(flattenRow.containsRealRowOf(state));
        
        //assertEquals(ApiTestBase.getLong(flattenRow, 0), Long.valueOf(1));
        assertEquals(flattenRow.value(0).getInt32(), 1);
        assertEquals(flattenRow.value(1).getString(), "fred");
        assertEquals(ApiTestBase.getLong(flattenRow, 2), Long.valueOf(1000));
        assertEquals(ApiTestBase.getLong(flattenRow, 3), Long.valueOf(1));
        assertEquals(ApiTestBase.getLong(flattenRow, 4), Long.valueOf(45));
    }
    
    @Test
    public void testProductRow() {
        Schema schema = caoiSchema();
        
        Table customer = schema.ais().getTable("schema", "customer");
        Table order = schema.ais().getTable("schema", "order");
        
        RowType customerType = schema.tableRowType(customer);
        RowType orderType = schema.tableRowType(order);
        
        ValuesHolderRow customerRow = new ValuesHolderRow(customerType, new Integer(1), new String("Fred"));
        ValuesHolderRow ordersRow = new ValuesHolderRow(orderType, new Integer(1000), new Integer(1), new Integer(45));
        
        ProductRowType productType = schema.newProductType(customerType, (TableRowType)customerType, orderType);
        
        ProductRow productRow = new ProductRow (productType, customerRow, ordersRow);
        
        assertNotNull (productRow);
       
        assertEquals(ApiTestBase.getLong(productRow, 0), Long.valueOf(1));
        assertEquals(productRow.value(1).getString(), "Fred");
        assertEquals(ApiTestBase.getLong(productRow, 2), Long.valueOf(45));
        
    }

    private Schema caoiSchema() {
        TestAISBuilder builder = new TestAISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "customer", "customer_name", 1, "MCOMPAT", "varchar", 64L, null, false);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "order", "customer_id", 1, "MCOMPAT", "int", false);
        builder.column("schema", "order", "order_date", 2, "MCOMPAT", "int", false);
        builder.pk("schema", "order");
        builder.indexColumn("schema", "order", Index.PRIMARY, "order_id", 0, true, null);
        builder.table("schema", "item");
        builder.column("schema", "item", "item_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "item", "order_id", 1, "MCOMPAT", "int", false);
        builder.column("schema", "item", "quantity", 2, "MCOMPAT", "int", false);
        builder.pk("schema", "item");
        builder.indexColumn("schema", "item", Index.PRIMARY, "item_id", 0, true, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "item_id");
        builder.table("schema", "state");
        builder.column("schema", "state", "code", 0, "MCOMPAT", "varchar", 2L, null, false);
        builder.column("schema", "state", "name", 1, "MCOMPAT", "varchar", 50L, null, false);
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
