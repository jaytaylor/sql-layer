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

package com.akiban.ais.model.validation;

import java.util.Collection;
import java.util.LinkedList;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Index;
import com.akiban.server.error.ErrorCode;



public class JoinToOneParentTest {
    private LinkedList<AISValidation>validations;
    private AISBuilder builder; 
    @Before 
    public void createValidations() {
        validations = new LinkedList<AISValidation>();
        validations.add(AISValidations.JOIN_TO_ONE_PARENT);
        
        builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "customer_name", 1, "varchar", 64L, 0L, false, false, null, null);
        builder.index("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("schema", "customer", Index.PRIMARY_KEY_CONSTRAINT, "customer_id", 0, true, null);
        builder.userTable("schema", "order");
        builder.column("schema", "order", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "customer_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addJoinToGroup("group", "co", 0);
        builder.groupingIsComplete();
        
    }

    @Test
    public void testValidJoins() {
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(validations).failures().size());
    }
    
    @Test 
    public void testTwoJoinsToOneParent() {
        builder.joinTables("co2", "schema", "customer", "schema", "order");
        builder.joinColumns("co2", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.groupingIsComplete();
        Collection<AISValidationFailure> failures = builder.akibanInformationSchema().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_MULTIPLE_PARENTS, fail.errorCode());
    }
    
    @Test
    public void testTwoJoinsToTwoParents() {
        builder.userTable("schema", "address");
        builder.column("schema", "address", "order_id", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "address", "customer_id", 1, "int", 0L, 0L, false, false, null, null);
        builder.joinTables("ca", "schema", "customer", "schema", "address");
        builder.joinColumns("ca", "schema", "customer", "customer_id", "schema", "address", "customer_id");
        builder.joinTables("oa", "schema", "order", "schema", "address");
        builder.joinColumns("oa", "schema", "order", "order_id", "schema", "address", "order_id");
        builder.basicSchemaIsComplete();
        builder.addJoinToGroup("group", "ca", 0);
        //builder.addJoinToGroup("group", "oa", 0);
        builder.groupingIsComplete();
        Collection<AISValidationFailure> failures = builder.akibanInformationSchema().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_MULTIPLE_PARENTS, fail.errorCode());
    }
}
