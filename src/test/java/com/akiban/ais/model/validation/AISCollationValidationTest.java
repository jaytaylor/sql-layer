
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
        validations = new LinkedList<>();
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
