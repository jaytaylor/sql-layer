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

import java.util.LinkedList;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.collation.AkCollatorFactory.Mode;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;
import com.foundationdb.server.error.UnsupportedCollationException;

public class AISCollationValidationTest {
    private LinkedList<AISValidation> validations;

    @Before
    public void createValidations() {
        validations = new LinkedList<>();
        validations.add(AISValidations.COLLATION_SUPPORTED);
    }

    private final TypesRegistry typesRegistry = TestTypesRegistry.MCOMPAT;

    @Test
    public void testSupportedCollation() {
        final TestAISBuilder builder = new TestAISBuilder(typesRegistry);
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, "MCOMPAT", "VARCHAR", 16L, true, null, "en_us");
        builder.basicSchemaIsComplete();
        Assert.assertEquals("Expect no validation failure for supported collation", 0, builder
                .akibanInformationSchema().validate(validations).failures().size());
    }

    @Test
    public void testUnsupportedCollationStrictMode() {
        Mode save = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode(Mode.STRICT);
            final TestAISBuilder builder = new TestAISBuilder(typesRegistry);
            builder.table("test", "t1");
            builder.column("test", "t1", "c1", 0, "MCOMPAT", "VARCHAR", 16L, true, null, "fricostatic_sengalese_ci");
            builder.basicSchemaIsComplete();
            Assert.assertEquals("Expect validation failure on invalid collation", 1, builder.akibanInformationSchema()
                    .validate(validations).failures().size());
        } catch (UnsupportedCollationException ex) {
            // Okay if thrown earlier.
        } finally {
            AkCollatorFactory.setCollationMode(save);
        }
    }

    @Test
    public void testUnsupportedCollationLooseMode() {
        Mode save = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode(Mode.LOOSE);
            final TestAISBuilder builder = new TestAISBuilder(typesRegistry);
            builder.table("test", "t1");
            builder.column("test", "t1", "c1", 0, "MCOMPAT", "VARCHAR", 16L, true, null, "fricostatic_sengalese_ci");
            builder.basicSchemaIsComplete();
            Assert.assertEquals("Expect no validation failure in loose mode", 0, builder.akibanInformationSchema()
                    .validate(validations).failures().size());
        } finally {
            AkCollatorFactory.setCollationMode(save);
        }
    }

}
