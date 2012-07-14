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

import java.util.LinkedList;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.collation.AkCollatorFactory.Mode;

public class AISCollationValidationTest {
    private LinkedList<AISValidation> validations;

    @Before
    public void createValidations() {
        validations = new LinkedList<AISValidation>();
        validations.add(AISValidations.COLLATION_SUPPORTED);
    }

    @Test
    public void testSupportedCollation() {
        final AISBuilder builder = new AISBuilder();
        builder.userTable("test", "t1");
        builder.column("test", "t1", "c1", 0, "INT", (long) 0, (long) 0, false, true, null, "latin1_swedish_ci");
        builder.basicSchemaIsComplete();
        Assert.assertEquals("Expect no validation failure for supported collation", 0, builder
                .akibanInformationSchema().validate(validations).failures().size());
    }

    @Test
    public void testUnsupportedCollationStrictMode() {
        Mode save = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode(Mode.STRICT);
            final AISBuilder builder = new AISBuilder();
            builder.userTable("test", "t1");
            builder.column("test", "t1", "c1", 0, "INT", (long) 0, (long) 0, false, true, null,
                    "fricostatic_sengalese_ci");
            builder.basicSchemaIsComplete();
            Assert.assertEquals("Expect validation failure on invalid collation", 1, builder.akibanInformationSchema()
                    .validate(validations).failures().size());
        } finally {
            AkCollatorFactory.setCollationMode(save);
        }
    }

    @Test
    public void testUnsupportedCollationLooseMode() {
        Mode save = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode(Mode.LOOSE);
            final AISBuilder builder = new AISBuilder();
            builder.userTable("test", "t1");
            builder.column("test", "t1", "c1", 0, "INT", (long) 0, (long) 0, false, true, null,
                    "fricostatic_sengalese_ci");
            builder.basicSchemaIsComplete();
            Assert.assertEquals("Expect no validation failure in loose mode", 0, builder.akibanInformationSchema()
                    .validate(validations).failures().size());
        } finally {
            AkCollatorFactory.setCollationMode(save);
        }
    }

}
