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

package com.akiban.qp.rowtype;

import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.PValuesRow;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.error.NotNullViolationException;

public class UserTableRowCheckerTest {
    
    @Test (expected = NotNullViolationException.class)
    public void idCheckNull () {
        Schema schema = caoiSchema();
        UserTable customer = schema.ais().getUserTable("schema", "customer");
        PValue col1 = new PValue(customer.getColumn(0).tInstance());
        col1.putNull();
        
        PValue col2 = new PValue(customer.getColumn(1).tInstance());
        col2.putString("Test Value", null);
        
        PValuesRow row = new PValuesRow (schema.userTableRowType(customer), col1, col2);
        
        UserTableRowChecker checker = new UserTableRowChecker (customer);
        checker.checkConstraints(row, true);
    }

    @Test (expected = NotNullViolationException.class)
    public void nameCheckNull () {
        Schema schema = caoiSchema();
        UserTable customer = schema.ais().getUserTable("schema", "customer");
        PValue col1 = new PValue(customer.getColumn(0).tInstance());
        col1.putInt32(1);
        
        PValue col2 = new PValue(customer.getColumn(1).tInstance());
        col2.putNull();
        
        PValuesRow row = new PValuesRow (schema.userTableRowType(customer), col1, col2);
        
        UserTableRowChecker checker = new UserTableRowChecker (customer);
        checker.checkConstraints(row, true);
    }
    
    
    private Schema caoiSchema() {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        
        builder.sequence("schema", "customer_id", 0, 1, 0, 1000, true);
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.columnAsIdentity("schema", "customer", "customer_id", "customer_id", true);
        
        
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, "customer_id", 0, true, null);
        builder.userTable("schema", "order");
        builder.column("schema", "order", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.sequence("schema", "order_id", 0, 1, 0, 1000, false);
        builder.columnAsIdentity("schema", "order", "order_id", "order_id", true);
        
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
        builder.userTable("schema", "address");
        builder.column("schema", "address", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "address", "location", 1, "varchar", 50L, 0L, false, false, null, null);
        builder.column("schema", "address", "zipcode", 2, "int", 0L, 0L, false, false, null, null);
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
