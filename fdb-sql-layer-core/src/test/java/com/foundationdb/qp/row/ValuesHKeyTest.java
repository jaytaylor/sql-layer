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
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.service.TypesRegistryServiceImpl;
import com.persistit.Key;

public class ValuesHKeyTest {


    @Before 
    public void createSchema() {
        TypesRegistryServiceImpl registryImpl = new TypesRegistryServiceImpl();
        registryImpl.start();
        registry = registryImpl;
        this.schema = caoiSchema();
    }
    
    @Test
    public void testSegmentsTop() {
        Table customer = schema.ais().getTable("schema", "customer");
        ValuesHKey key = new ValuesHKey(schema.newHKeyRowType(customer.hKey()), registry);
        assertEquals(key.segments(), 1);
        assertEquals(key.rowType().nFields(), 1);
    }
    
    @Test
    public void testSegmentsBottom() {
        Table item = schema.ais().getTable("schema", "item");
        ValuesHKey key = new ValuesHKey(schema.newHKeyRowType(item.hKey()), registry);
        assertEquals(key.segments(), 3);
        assertEquals(key.rowType().nFields(), 3);
    }
    
    @Test
    public void ordinalsSetCorrectly() {
        ValuesHKey key = createHKey("item");

        assertEquals(key.segments(), 3);
        
        assertEquals(key.ordinals()[0], ordinal("customer"));
        assertEquals(key.ordinals()[1], ordinal("order"));
        assertEquals(key.ordinals()[2], ordinal("item"));
    }

    @Test
    public void copyToOneLevel() {
        ValuesHKey key = createHKey("customer");
        key.valueAt(0).putInt32(42);

        ValuesHKey target = createHKey("customer");
        
        key.copyTo(target);
        
        assertEquals(target.valueAt(0).getInt32(), 42);
    }
    
    @Test
    public void copyToThreeSegments() {
        ValuesHKey key = createHKey("item");
        // CustomerID 
        key.valueAt(0).putInt32(42);
        // OrderID
        key.valueAt(1).putInt32(51);
        
        ValuesHKey target = createHKey("item");
        key.copyTo(target);
        
        assertEquals (target.valueAt(0).getInt32(), 42);
        assertEquals (target.valueAt(1).getInt32(), 51);
        assertEquals (target.segments(), 3);
    }

    @Test
    public void copyToOnTwoSegment() {
        ValuesHKey key = createHKey("item");
        // OrderID 
        key.valueAt(0).putInt32(42);
        // ItemID
        key.valueAt(1).putInt32(51);
        key.useSegments(2);
        
        ValuesHKey target = createHKey("item");
        key.copyTo(target);
        
        assertEquals (target.valueAt(0).getInt32(), 42);
        assertEquals (target.valueAt(1).getInt32(), 51);
        assertEquals (target.segments(), 2);
    }

    @Test
    public void copyToOnOneSegment() {
        ValuesHKey key = createHKey("item");
        // OrderID 
        key.valueAt(0).putInt32(42);
        // ItemID
        key.valueAt(1).putInt32(51);
        key.useSegments(1);
        
        ValuesHKey target = createHKey("item");
        key.copyTo(target);
        
        assertEquals (target.valueAt(0).getInt32(), 42);
        assertTrue (!target.valueAt(1).hasAnyValue());
        assertEquals(target.segments(), 1);
    }
    
    @Test
    public void copyToDownCastTwo() {
        ValuesHKey key = createHKey("customer");
        key.valueAt(0).putInt32(42);
        
        ValuesHKey target = createHKey("item");
        key.copyTo(target);
        assertEquals (target.valueAt(0).getInt32(), 42);
        assertTrue (!target.valueAt(1).hasAnyValue());
        assertEquals(target.segments(), 1);
    }

    @Test
    public void copyToDownCastOne() {
        ValuesHKey key = createHKey("customer");
        key.valueAt(0).putInt32(42);
        ValuesHKey target = createHKey("order");
        key.copyTo(target);
        assertEquals(target.valueAt(0).getInt32(),42);
        assertEquals(target.segments(), 1);
        
    }    
    
    @Test
    public void prefixOf1() {
        ValuesHKey key = createHKey("item");
        key.valueAt(0).putInt32(42);
        key.valueAt(1).putInt32(51);

        ValuesHKey customerKey = createHKey("customer");
        customerKey.valueAt(0).putInt32(42);
        
        assertTrue (customerKey.prefixOf(key));
    }

    @Test
    public void prefixOf2() {
        ValuesHKey key = createHKey("item");
        key.valueAt(0).putInt32(42);
        key.valueAt(1).putInt32(51);

        ValuesHKey orderKey = createHKey("order");
        orderKey.valueAt(0).putInt32(42);
        orderKey.valueAt(1).putInt32(51);
        
        assertTrue (orderKey.prefixOf(key));
    }
    
    @Test
    public void prefixOfFail1() {
        ValuesHKey key = createHKey("item");
        key.valueAt(0).putInt32(42);
        key.valueAt(1).putInt32(51);

        ValuesHKey customerKey = createHKey("customer");
        customerKey.valueAt(0).putInt32(44);
        
        assertTrue (!customerKey.prefixOf(key));
    }
    
    @Test
    public void prefixOfFail2() {
        ValuesHKey key = createHKey("state");
        key.valueAt(0).putInt64(42);
        
        ValuesHKey customerKey = createHKey("customer");
        customerKey.valueAt(0).putInt32(42);
        
        assertTrue (!customerKey.prefixOf(key));
    }
    
    @Test
    public void prefixOfSegments1() {
        ValuesHKey key = createHKey("item");
        key.valueAt(0).putInt32(42);
        key.valueAt(1).putInt32(51);

        ValuesHKey itemKey = createHKey("item");
        itemKey.valueAt(0).putInt32(42);
        
        itemKey.useSegments(1);
        
        assertTrue (itemKey.prefixOf(key));
    }

    @Test
    public void prefixOfSegments2() {
        ValuesHKey key = createHKey("item");
        key.valueAt(0).putInt32(42);
        key.valueAt(1).putInt32(51);

        ValuesHKey itemKey = createHKey("item");
        itemKey.valueAt(0).putInt32(42);
        itemKey.valueAt(1).putInt32(51);
        
        itemKey.useSegments(1);
        
        assertTrue (!key.prefixOf(itemKey));
    }

    @Test
    public void copyFromKeyCustomer () {
        Key key = new Key (null, 2047);
        key.append(ordinal("customer"));
        key.append(42L);
        
        ValuesHKey customerKey = createHKey("customer");
        customerKey.copyFrom(key);
        
        assertEquals(customerKey.valueAt(0).getInt32(), 42);
        assertEquals (customerKey.values.size(), 1);
    }
    
    @Test
    public void copyFromKeyOrder () {
        Key key = new Key (null, 2047);
        key.append(ordinal("customer"));
        key.append(42L);
        key.append(ordinal("order"));
        key.append(51L);
        
        ValuesHKey orderKey = createHKey("order");
        orderKey.copyFrom(key);
        
        assertEquals (orderKey.segments(), 2);
        assertEquals (orderKey.valueAt(0).getInt32(), 42);
        assertEquals (orderKey.valueAt(1).getInt32(), 51);
        assertEquals (orderKey.values.size(), 2);
    }

    
    @Test
    public void copyFromKeyItem () {
        Key key = new Key (null, 2047);
        key.append(ordinal("customer"));
        key.append(42L);
        key.append(ordinal("order"));
        key.append(51L);
        key.append(ordinal("item"));
        key.append(99L);
        
        ValuesHKey itemKey = createHKey("item");
        itemKey.copyFrom(key);
        
        assertEquals (itemKey.segments(), 3);
        assertEquals (itemKey.values.size(), 3);
        assertEquals (itemKey.valueAt(0).getInt32(), 42);
        assertEquals (itemKey.valueAt(1).getInt32(), 51);
        assertEquals (itemKey.valueAt(2).getInt32(), 99);
    }

    
    @Test
    public void copyToKeyCustomer() {
        ValuesHKey customerKey = createHKey("customer");
        customerKey.valueAt(0).putInt32(42);

        Key key = new Key (null, 2047);
        customerKey.copyTo(key);
        
        key.indexTo(0);
        assertEquals (key.decodeInt(), ordinal("customer"));
        assertEquals (key.decodeLong(), 42);
    }
    
    @Test
    public void copyToKeyItem() {
        ValuesHKey key = createHKey("item");
        key.valueAt(0).putInt32(42);
        key.valueAt(1).putInt32(51);

        Key target = new Key (null, 2047);
        key.copyTo(target);
        
        target.indexTo(0);
        assertEquals(target.decodeInt(), ordinal("customer"));
        assertEquals(target.decodeLong(), 42);
        assertEquals(target.decodeInt(), ordinal("order"));
        assertEquals(target.decodeLong(), 51);
        assertEquals(target.decodeInt(), ordinal("item"));
    }
    
    @Test
    public void extendWithNull() {
        ValuesHKey key = createHKey("customer");
        key.valueAt(0).putInt32(42);
        ValuesHKey target = createHKey("order");
        key.copyTo(target);
        assertEquals(target.valueAt(0).getInt32(),42);
        assertEquals(target.segments(), 1);
        
        target.extendWithOrdinal(ordinal("order"));
        assertEquals(target.segments(), 2);
        target.extendWithNull();
        assertTrue (target.valueAt(1).isNull());
        
    }
    
    @Test
    public void extendOrdinalTwo() {
        ValuesHKey key = createHKey("customer");
        key.valueAt(0).putInt32(42);
        
        ValuesHKey target = createHKey("item");
        key.copyTo(target);
        
        assertEquals (target.valueAt(0).getInt32(), 42);
        assertTrue (!target.valueAt(1).hasAnyValue());
        assertTrue(!target.valueAt(2).hasAnyValue());
        assertEquals(target.segments(), 1);

        target.extendWithOrdinal(ordinal("item"));
        assertEquals (target.segments(), 3);
        assertTrue(target.valueAt(1).isNull());
        assertTrue(!target.valueAt(2).hasAnyValue());
    }
    
    @Test
    public void compareTypes() {
        ValuesHKey key= createHKey("customer");
        key.valueAt(0).putInt32(42);
        
        ValuesHKey target = createHKey ("customer2");
        target.valueAt(0).putInt64(42);
        
        assertEquals(key.compareTo(target), 0);
        assertEquals(target.compareTo(key), 0);
    }
    
    private ValuesHKey createHKey(String tableName) {
        
        Table table = schema.ais().getTable("schema", tableName);
        ValuesHKey key = new ValuesHKey(schema.newHKeyRowType(table.hKey()), registry);
        return key;
    }
    
    private int ordinal (String tableName) {
        return schema.ais().getTable("schema", tableName).getOrdinal().intValue();
    }
    
    private Schema caoiSchema() {
        TestAISBuilder builder = new TestAISBuilder(registry.getTypesRegistry());
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
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "order_id");
        builder.table("schema", "state");
        builder.column("schema", "state", "code", 0, "MCOMPAT", "varchar", 2L, null, false);
        builder.column("schema", "state", "name", 1, "MCOMPAT", "varchar", 50L, null, false);
        builder.table("schema", "customer2");
        builder.column("schema", "customer2", "customer_id", 0, "MCOMPAT", "int unsigned", false);
        builder.column("schema", "customer2", "customer_name", 1, "MCOMPAT", "VARCHAR", 64L, null,  false);
        builder.pk("schema", "customer2");
        builder.indexColumn("schema", "customer2", Index.PRIMARY, "customer_id", 0, true, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "schema");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        builder.createGroup("state", "schema");
        builder.addTableToGroup("state", "schema", "state");
        builder.createGroup("customer", "schema");
        builder.addTableToGroup("customer", "schema", "customer2");
        builder.groupingIsComplete();
        
        SchemaFactory factory = new SchemaFactory ("schema");
        factory.buildRowDefs(builder.akibanInformationSchema());
        return new Schema(builder.akibanInformationSchema());
    }

    private Schema schema;
    private TypesRegistryService registry;

}
