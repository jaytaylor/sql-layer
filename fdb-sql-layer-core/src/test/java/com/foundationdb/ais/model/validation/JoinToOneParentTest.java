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

package com.foundationdb.ais.model.validation;

import java.util.Collection;
import java.util.LinkedList;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.types.service.TestTypesRegistry;


public class JoinToOneParentTest {
    private LinkedList<AISValidation>validations;
    private TestAISBuilder builder; 

    @Before 
    public void createValidations() {
        validations = new LinkedList<>();
        validations.add(AISValidations.JOIN_TO_ONE_PARENT);
        
        builder = new TestAISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "customer", "customer_name", 1, "MCOMPAT", "varchar", 64L, null, false);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "order", "customer_id", 1, "MCOMPAT", "int", false);
        builder.column("schema", "order", "order_date", 2, "MCOMPAT", "int", false);
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
        builder.table("schema", "address");
        builder.column("schema", "address", "order_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "address", "customer_id", 1, "MCOMPAT", "int", false);
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
