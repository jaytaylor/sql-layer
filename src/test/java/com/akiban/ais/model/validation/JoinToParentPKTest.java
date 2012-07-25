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

import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.error.ErrorCode;

public class JoinToParentPKTest {
    private LinkedList<AISValidation>validations;
    private NewAISBuilder builder;

    @Before 
    public void createValidations () {
        validations = new LinkedList<AISValidation>();
        validations.add(AISValidations.JOIN_TO_PARENT_PK);
        validations.add(AISValidations.JOIN_COLUMN_TYPES_MATCH);

        builder = AISBBasedBuilder.create("test");
        builder.userTable("t1").colLong("c1").colString("c2", 10).pk("c1");
        builder.userTable("t2").colLong("c1").colString("c2", 10).pk("c1", "c2");
        builder.userTable("t3").colLong("c1").colString("c2", 10);
    }
    
    @Test
    public void joinOneColumnValid() {
        builder.userTable("j1").colLong("c1").colLong("c2").pk("c1").joinTo("t1").on("c2", "c1");
        Assert.assertEquals(0, 
                builder.unvalidatedAIS().validate(validations).failures().size());
    }
    
    @Test
    public void joinTwoColumnValid() {
        builder.userTable("j2").colLong("c1").colString("c2", 10).pk("c1").joinTo("t2").on("c1", "c1").and("c2", "c2");
        Assert.assertEquals(0, 
                builder.unvalidatedAIS().validate(validations).failures().size());
    }

    @Test
    public void joinNoPKFailed() {
        builder.userTable("j3").colLong("c1").joinTo("t3").on("c1", "c1");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_PARENT_NO_PK, fail.errorCode());
    }
    
    @Test
    public void joinOneToTwoMismatch() {
        builder.userTable("j4").colLong("c1").joinTo("t2").on("c1", "c1");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_COLUMN_MISMATCH, fail.errorCode());
    }
    
    @Test
    public void joinTwoToOneMismatch() { 
        builder.userTable("j5").colLong("c1").colString("c2", 10).joinTo("t1").on("c1","c1").and("c2", "c2");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_COLUMN_MISMATCH, fail.errorCode());
    }
    
    @Test
    public void joinColumnsMismatch () { 
        builder.userTable("j6").colLong("c1").colString("c2", 10).joinTo("t2").on("c2", "c1").and("c1", "c2");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(2, failures.size());
        
    }
    
    // bug #1014325 : This should fail, but doesn't
    @Test
    public void joinOrderMismatch() {
        builder.userTable("j7").colLong("c1").colString("c2", 10).joinTo("t2").on("c2", "c2").and("c1", "c1");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(0, failures.size());
    }
    
    @Test
    public void joinToNonPKColumns() {
        builder.userTable("j8").colLong("c1").colString("c2", 10).joinTo("t1").on("c2", "c2");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_WRONG_COLUMNS, fail.errorCode());

    }
}
