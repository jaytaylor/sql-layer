
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
        validations = new LinkedList<>();
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
    
    @Test
    public void joinOrderMismatch() {
        builder.userTable("j7").colLong("c1").colString("c2", 10).joinTo("t2").on("c2", "c2").and("c1", "c1");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(2, failures.size());
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
