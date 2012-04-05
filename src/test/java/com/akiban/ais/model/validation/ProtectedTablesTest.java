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
