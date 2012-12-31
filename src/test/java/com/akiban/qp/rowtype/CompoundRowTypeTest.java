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

package com.akiban.qp.rowtype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.server.types.AkType;

public class CompoundRowTypeTest {
    @Test
    public void testFlattenedTypeCreate() {
        Schema schema = caoiSchema();
        UserTable customer = schema.ais().getUserTable("schema", "customer");
        UserTable orders = schema.ais().getUserTable("schema", "order");

        FlattenedRowType type = schema.newFlattenType(schema.userTableRowType(customer),
                schema.userTableRowType(orders));
        assertNotNull (type);
        assertEquals (5, type.nFields());
        assertEquals (type.parentType(), schema.userTableRowType(customer));
        assertEquals (type.childType(), schema.userTableRowType(orders));
        
        assertEquals (type.typeAt(0), AkType.INT);
        assertEquals (type.typeAt(1), AkType.VARCHAR);
        assertEquals (type.typeAt(2), AkType.INT);
        assertEquals (type.typeAt(3), AkType.INT );
        assertEquals (type.typeAt(4), AkType.INT);
        
        assertTrue (type.parentType().parentOf(type.childType()));
        //assertTrue (type.childType().ancestorOf(type.parentType()));
    }
    
    @Test
    public void testFlattenTypeSkip() {
        Schema schema = caoiSchema();
        UserTable customer = schema.ais().getUserTable("schema", "customer");
        UserTable items = schema.ais().getUserTable("schema", "item");
        
        FlattenedRowType type = schema.newFlattenType(schema.userTableRowType(customer), 
                                schema.userTableRowType(items));
        assertNotNull (type);
        assertEquals (5, type.nFields());
        assertEquals (type.typeAt(0), AkType.INT);
        assertEquals (type.typeAt(1), AkType.VARCHAR);
        assertEquals (type.typeAt(2), AkType.INT);
        assertEquals (type.typeAt(3), AkType.INT );
        assertEquals (type.typeAt(4), AkType.INT);
        
        assertTrue (type.parentType().parentOf(type.childType()));
    }

    @Test
    public void testProductTypeCreation() {
        Schema schema = caoiSchema();
        UserTable customer = schema.ais().getUserTable("schema", "customer");
        
        UserTable orders = schema.ais().getUserTable("schema", "order");
        
        FlattenedRowType flatType = schema.newFlattenType(schema.userTableRowType(customer), 
                schema.userTableRowType(orders));

        ProductRowType type = schema.newProductType(schema.userTableRowType(customer), 
                                schema.userTableRowType(customer), 
                                flatType);
        assertNotNull (type);
        assertEquals (type.leftType(), schema.userTableRowType(customer));
        assertEquals (type.rightType(), flatType);
        assertEquals (5, type.nFields());
        assertFalse (type.hasUserTable());
        
        assertEquals (type.typeAt(0), AkType.INT);
        assertEquals (type.typeAt(1), AkType.VARCHAR);
        assertEquals (type.typeAt(2), AkType.INT);
        assertEquals (type.typeAt(3), AkType.INT);
        assertEquals (type.typeAt(4), AkType.INT);
    }
    
    @Test
    public void testProductTypeSame() {
        Schema schema = caoiSchema();
        UserTable orders = schema.ais().getUserTable("schema", "order");
        ProductRowType type = schema.newProductType(schema.userTableRowType(orders), 
                null, 
                schema.userTableRowType(orders));
        assertNotNull (type);
        assertFalse (type.hasUserTable());
        assertEquals(3, type.nFields());
        assertEquals(type.typeAt(0), AkType.INT);
        assertEquals(type.typeAt(1), AkType.INT);
        assertEquals(type.typeAt(2), AkType.INT);
    }
    
    @Test
    public void testProductTypeBranch() {
        Schema schema = caoiSchema();
        UserTable customer = schema.ais().getUserTable("schema", "customer");
        UserTable orders = schema.ais().getUserTable("schema", "order");
        UserTable addresses = schema.ais().getUserTable("schema", "address");

        FlattenedRowType flatOrder = schema.newFlattenType(schema.userTableRowType(customer), 
                schema.userTableRowType(orders));
        
        ProductRowType type = schema.newProductType(flatOrder, 
                schema.userTableRowType(customer), 
                schema.userTableRowType(addresses));
        assertNotNull (type);
        assertEquals (7, type.nFields());
        assertEquals(type.typeAt(0), AkType.INT);
        assertEquals(type.typeAt(1), AkType.VARCHAR);
        assertEquals(type.typeAt(2), AkType.INT);
        assertEquals(type.typeAt(3), AkType.INT);
        assertEquals(type.typeAt(4), AkType.INT);
        assertEquals(type.typeAt(5), AkType.INT);
        assertEquals(type.typeAt(6), AkType.LONG);
        
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
