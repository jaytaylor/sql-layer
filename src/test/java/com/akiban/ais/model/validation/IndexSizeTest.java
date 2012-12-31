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

public class IndexSizeTest {
    private LinkedList<AISValidation>validations;
    private AISBuilder builder;
    @Before 
    public void createValidations() {
        validations = new LinkedList<AISValidation>();
        validations.add(AISValidations.INDEX_SIZES);
        
        builder = new AISBuilder();
        builder.userTable("test", "t1");
        builder.column("test", "t1", "c1", 0, "varchar", 50L, 0L, false, false, null, null);
        builder.column("test", "t1", "c2", 1, "varchar", 2000L, 0L, false, false, null, null);
        builder.column("test", "t1", "c3", 2, "varchar", 200L, 0L, false, false, null, null);
        builder.index("test", "t1", "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("test", "t1", "PRIMARY", "c1", 0, true, null);
        
    }

    @Test
    public void normalIndex() {
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(validations).failures().size());
    }
    
    @Test
    public void indexSizeTooLarge() {
        builder.index("test", "t1", "i1", true, Index.UNIQUE_KEY_CONSTRAINT);
        builder.indexColumn("test", "t1", "i1", "c2", 0, true, null);
        builder.createGroup("t1", "test");
        builder.addTableToGroup("t1", "test", "t1");
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        Collection<AISValidationFailure> failures = builder.akibanInformationSchema().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.UNSUPPORTED_INDEX_SIZE, fail.errorCode());
        
    }
    
    @Test 
    public void indexPrefixRejected() {
        builder.index("test", "t1", "i1", true, Index.UNIQUE_KEY_CONSTRAINT);
        builder.indexColumn("test", "t1", "i1", "c2", 0, true, 1024);
        builder.createGroup("t1", "test");
        builder.addTableToGroup("t1", "test", "t1");
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        Collection<AISValidationFailure> failures = builder.akibanInformationSchema().validate(validations).failures();
        Assert.assertEquals(2, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.UNSUPPORTED_INDEX_PREFIX, fail.errorCode());
    }
    
    @Test
    public void groupIndexTooLarge() {
        builder.createGroup("t1", "test");
        builder.addTableToGroup("t1", "test", "t1");
        builder.groupIndex("t1", "i1", false, null);
        builder.groupIndexColumn("t1", "i1", "test", "t1", "c2", 0);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        Collection<AISValidationFailure> failures = builder.akibanInformationSchema().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.UNSUPPORTED_INDEX_SIZE, fail.errorCode());
        
    }
    
    @Test
    public void hkeyOK() {
        builder.userTable("test", "t2");
        builder.column("test", "t2", "c1", 0, "varchar", 50L, 0L, false, false, null, null);
        builder.column("test", "t2", "c2", 1, "varchar", 2000L, 0L, false, false, null, null);
        builder.index("test", "t2", "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("test", "t2", "PRIMARY", "c2", 0, true, null);
        builder.createGroup("t2", "test");
        builder.addTableToGroup("t2", "test", "t2");

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        Collection<AISValidationFailure> failures = builder.akibanInformationSchema().validate(validations).failures();
        Assert.assertEquals(0, failures.size());
    }
    
    @Test
    public void hkeyTooLarge() {
        builder.userTable("test", "t2");
        builder.column("test", "t2", "c1", 0, "varchar", 50L, 0L, false, false, null, null);
        builder.column("test", "t2", "c2", 1, "varchar", 2000L, 0L, false, false, null, null);
        builder.index("test", "t2", "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("test", "t2", "PRIMARY", "c2", 0, true, null);
        
        builder.joinTables("t2/t1", "test", "t2", "test", "t1");
        builder.joinColumns("t2/t1", "test", "t2", "c2", "test", "t1", "c2");
        builder.basicSchemaIsComplete();
        builder.createGroup("t3", "test");
        builder.addJoinToGroup("t3", "t2/t1", 0);
        builder.groupingIsComplete();
        Collection<AISValidationFailure> failures = builder.akibanInformationSchema().validate(validations).failures();
        Assert.assertEquals(2, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.UNSUPPORTED_INDEX_SIZE, fail.errorCode());
    }
}
