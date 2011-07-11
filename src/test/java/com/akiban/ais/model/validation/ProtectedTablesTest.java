/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */
package com.akiban.ais.model.validation;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.TableName;

public class ProtectedTablesTest {

    private LinkedList<AISValidation>validations;
    
    @Before 
    public void createValidations() {
        validations = new LinkedList<AISValidation>();
        validations.add(AISValidations.PROTECTED_TABLES);
    }
    
    @Test
    public void validTablesInAIS () {
        AISBuilder builder = new AISBuilder();
        builder.userTable(TableName.AKIBAN_INFORMATION_SCHEMA, "tables");

        AISValidationResults results = builder.akibanInformationSchema().validate(validations);
        assertEquals(0, results.failures().size());
        
    }
    
    @Test
    public void invalidTablesInAIS() {
        AISBuilder builder = new AISBuilder();
        builder.userTable(TableName.AKIBAN_INFORMATION_SCHEMA, "foo");
        AISValidationResults results = builder.akibanInformationSchema().validate(validations);
        assertEquals(1, results.failures().size());
    }
    
    @Test 
    public void mixedTablesInAIS() {
        AISBuilder builder = new AISBuilder();
        builder.userTable(TableName.AKIBAN_INFORMATION_SCHEMA, "tables");
        builder.userTable(TableName.AKIBAN_INFORMATION_SCHEMA, "indexes");
        builder.userTable(TableName.AKIBAN_INFORMATION_SCHEMA, "mytable");
        builder.userTable("test", "mytable");
        
        AISValidationResults results = builder.akibanInformationSchema().validate(validations);
        assertEquals(1, results.failures().size());
    }
}
