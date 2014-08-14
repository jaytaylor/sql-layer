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

import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;

public class JoinToParentPKTest {
    private LinkedList<AISValidation>validations;
    private NewAISBuilder builder;

    @Before 
    public void createValidations () {
        validations = new LinkedList<>();
        validations.add(AISValidations.JOIN_TO_PARENT_PK);
        validations.add(AISValidations.JOIN_COLUMN_TYPES_MATCH);

        builder = AISBBasedBuilder.create("test", MTypesTranslator.INSTANCE);
        builder.table("t1").colInt("c1").colString("c2", 10).pk("c1");
        builder.table("t2").colInt("c1").colString("c2", 10).pk("c1", "c2");
        builder.table("t3").colInt("c1").colString("c2", 10);
    }
    
    @Test
    public void joinOneColumnValid() {
        builder.table("j1").colInt("c1").colInt("c2").pk("c1").joinTo("t1").on("c2", "c1");
        Assert.assertEquals(0, 
                builder.unvalidatedAIS().validate(validations).failures().size());
    }
    
    @Test
    public void joinTwoColumnValid() {
        builder.table("j2").colInt("c1").colString("c2", 10).pk("c1").joinTo("t2").on("c1", "c1").and("c2", "c2");
        Assert.assertEquals(0, 
                builder.unvalidatedAIS().validate(validations).failures().size());
    }

    @Test
    public void joinNoPKFailed() {
        builder.table("j3").colInt("c1").joinTo("t3").on("c1", "c1");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_PARENT_NO_PK, fail.errorCode());
    }
    
    @Test
    public void joinOneToTwoMismatch() {
        builder.table("j4").colInt("c1").joinTo("t2").on("c1", "c1");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_COLUMN_MISMATCH, fail.errorCode());
    }
    
    @Test
    public void joinTwoToOneMismatch() { 
        builder.table("j5").colInt("c1").colString("c2", 10).joinTo("t1").on("c1","c1").and("c2", "c2");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_COLUMN_MISMATCH, fail.errorCode());
    }
    
    @Test
    public void joinColumnsMismatch () { 
        builder.table("j6").colInt("c1").colString("c2", 10).joinTo("t2").on("c2", "c1").and("c1", "c2");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(2, failures.size());
        
    }
    
    @Test
    public void joinOrderMismatch() {
        builder.table("j7").colInt("c1").colString("c2", 10).joinTo("t2").on("c2", "c2").and("c1", "c1");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(2, failures.size());
    }
    
    @Test
    public void joinToNonPKColumns() {
        builder.table("j8").colInt("c1").colString("c2", 10).joinTo("t1").on("c2", "c2");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_WRONG_COLUMNS, fail.errorCode());

    }
}
