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

package com.foundationdb.qp.rowtype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.service.TestTypesRegistry;

public class CompoundRowTypeTest {
    
    @Test
    public void testFlattenedTypeCreate() {
        Schema schema = caoiSchema();
        Table customer = schema.ais().getTable("schema", "customer");
        Table orders = schema.ais().getTable("schema", "order");

        FlattenedRowType type = schema.newFlattenType(schema.tableRowType(customer),
                schema.tableRowType(orders));
        assertNotNull (type);
        assertEquals (5, type.nFields());
        assertEquals (type.parentType(), schema.tableRowType(customer));
        assertEquals (type.childType(), schema.tableRowType(orders));
        
        assertTrue(type.typeAt(0).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(1).equalsIncludingNullable(MString.VARCHAR.instance(64, false)));
        assertTrue(type.typeAt(2).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(3).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(4).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue (type.parentType().parentOf(type.childType()));
        //assertTrue (type.childType().ancestorOf(type.parentType()));
    }
    
    @Test
    public void testFlattenTypeSkip() {
        Schema schema = caoiSchema();
        Table customer = schema.ais().getTable("schema", "customer");
        Table items = schema.ais().getTable("schema", "item");
        
        FlattenedRowType type = schema.newFlattenType(schema.tableRowType(customer),
                                schema.tableRowType(items));
        assertNotNull (type);
        assertEquals (5, type.nFields());
        assertTrue(type.typeAt(0).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(1).equalsIncludingNullable(MString.VARCHAR.instance(64, false)));
        assertTrue(type.typeAt(2).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(3).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(4).equalsIncludingNullable(MNumeric.INT.instance(false)));
        
        assertTrue (type.parentType().parentOf(type.childType()));
    }

    @Test
    public void testProductTypeCreation() {
        Schema schema = caoiSchema();
        Table customer = schema.ais().getTable("schema", "customer");
        
        Table orders = schema.ais().getTable("schema", "order");
        
        FlattenedRowType flatType = schema.newFlattenType(schema.tableRowType(customer),
                schema.tableRowType(orders));

        ProductRowType type = schema.newProductType(schema.tableRowType(customer),
                                schema.tableRowType(customer),
                                flatType);
        assertNotNull (type);
        assertEquals (type.leftType(), schema.tableRowType(customer));
        assertEquals (type.rightType(), flatType);
        assertEquals (5, type.nFields());
        assertFalse (type.hasTable());
        
        assertTrue(type.typeAt(0).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(1).equalsIncludingNullable(MString.VARCHAR.instance(64, false)));
        assertTrue(type.typeAt(2).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(3).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(4).equalsIncludingNullable(MNumeric.INT.instance(false)));
    }
    
    @Test
    public void testProductTypeSame() {
        Schema schema = caoiSchema();
        Table orders = schema.ais().getTable("schema", "order");
        ProductRowType type = schema.newProductType(schema.tableRowType(orders),
                null, 
                schema.tableRowType(orders));
        assertNotNull (type);
        assertFalse (type.hasTable());
        assertEquals(3, type.nFields());
        assertTrue(type.typeAt(0).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(1).equalsIncludingNullable(MNumeric.INT.instance(false)));
        assertTrue(type.typeAt(2).equalsIncludingNullable(MNumeric.INT.instance(false)));
    }
    
    @Test
    public void testProductTypeBranch() {
        Schema schema = caoiSchema();
        Table customer = schema.ais().getTable("schema", "customer");
        Table orders = schema.ais().getTable("schema", "order");
        Table addresses = schema.ais().getTable("schema", "address");

        FlattenedRowType flatOrder = schema.newFlattenType(schema.tableRowType(customer),
                schema.tableRowType(orders));
        
        ProductRowType type = schema.newProductType(flatOrder, 
                schema.tableRowType(customer),
                schema.tableRowType(addresses));
        
        assertNotNull (type);
        assertEquals (7, type.nFields());
        assertEquals (type.typeAt(0), MNumeric.INT.instance(false));
        assertEquals (type.typeAt(1), MString.VARCHAR.instance(64, false));
        assertEquals (type.typeAt(2), MNumeric.INT.instance(false));
        assertEquals (type.typeAt(3), MNumeric.INT.instance(false));
        assertEquals (type.typeAt(4), MNumeric.INT.instance(false));
        assertEquals (type.typeAt(5), MNumeric.INT.instance(false));
        assertEquals (type.typeAt(6), MNumeric.BIGINT.instance(false));
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
        builder.table("schema", "address");
        builder.column("schema", "address", "customer_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "address", "location", 1, "MCOMPAT", "varchar", 50L, null, false);
        builder.column("schema", "address", "zipcode", 2, "MCOMPAT", "int", false);
        builder.joinTables("ca", "schema", "customer", "schema", "address");
        builder.joinColumns("ca", "schema", "customer", "customer_id", "schema", "address", "customer_id");
        
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        builder.addJoinToGroup("group", "ca", 0);
        builder.createGroup("state", "schema");
        builder.addTableToGroup("state", "schema", "state");
        builder.groupingIsComplete();
        
        SchemaFactory factory = new SchemaFactory ("schema");
        factory.buildRowDefs(builder.akibanInformationSchema());
        return new Schema(builder.akibanInformationSchema());
    }
}
