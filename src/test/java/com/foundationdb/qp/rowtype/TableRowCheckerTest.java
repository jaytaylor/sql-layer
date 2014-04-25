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

import com.foundationdb.qp.row.ValuesRow;
import com.foundationdb.server.types.value.Value;
import org.junit.Test;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.server.error.NotNullViolationException;
import com.foundationdb.server.types.service.TestTypesRegistry;

public class TableRowCheckerTest {
    
    @Test (expected = NotNullViolationException.class)
    public void idCheckNull () {
        Schema schema = caoiSchema();
        Table customer = schema.ais().getTable("schema", "customer");
        Value col1 = new Value(customer.getColumn(0).getType());
        col1.putNull();
        
        Value col2 = new Value(customer.getColumn(1).getType());
        col2.putString("Test Value", null);
        
        ValuesRow row = new ValuesRow(schema.tableRowType(customer), col1, col2);
        
        TableRowChecker checker = new TableRowChecker(customer);
        checker.checkConstraints(row);
    }

    @Test (expected = NotNullViolationException.class)
    public void nameCheckNull () {
        Schema schema = caoiSchema();
        Table customer = schema.ais().getTable("schema", "customer");
        Value col1 = new Value(customer.getColumn(0).getType());
        col1.putInt32(1);
        
        Value col2 = new Value(customer.getColumn(1).getType());
        col2.putNull();
        
        ValuesRow row = new ValuesRow(schema.tableRowType(customer), col1, col2);
        
        TableRowChecker checker = new TableRowChecker(customer);
        checker.checkConstraints(row);
    }
    
    
    private Schema caoiSchema() {
        TestAISBuilder builder = new TestAISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "customer");
        
        builder.sequence("schema", "customer_id", 0, 1, 0, 1000, true);
        builder.column("schema", "customer", "customer_id", 0, "MCOMPAT", "int", false);
        builder.columnAsIdentity("schema", "customer", "customer_id", "customer_id", true);
        
        
        builder.column("schema", "customer", "customer_name", 1, "MCOMPAT", "varchar", 64L, null, false);
        builder.index("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, "MCOMPAT", "int", false);
        builder.sequence("schema", "order_id", 0, 1, 0, 1000, false);
        builder.columnAsIdentity("schema", "order", "order_id", "order_id", true);
        
        builder.column("schema", "order", "customer_id", 1, "MCOMPAT", "int", false);
        builder.column("schema", "order", "order_date", 2, "MCOMPAT", "int", false);
        builder.index("schema", "order", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "order", Index.PRIMARY_KEY_CONSTRAINT, "order_id", 0, true, null);
        builder.table("schema", "item");
        builder.column("schema", "item", "item_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "item", "order_id", 1, "MCOMPAT", "int", false);
        builder.column("schema", "item", "quantity", 2, "MCOMPAT", "int", false);
        builder.index("schema", "item", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "item", Index.PRIMARY_KEY_CONSTRAINT, "item_id", 0, true, null);
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
