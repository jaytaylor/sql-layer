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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.error.IndexColumnIsPartialException;
import com.foundationdb.server.types.service.TestTypesRegistry;
import org.junit.Test;

import java.util.Collections;

public class IndexColumnIsNotPartialTest {
    private final static String SCHEMA = "test";
    private final static String TABLE = "t";

    private static AkibanInformationSchema createAIS(long fullLen, Integer indexedLength) {
        TestAISBuilder builder = new TestAISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "v", 0, "MCOMPAT", "VARCHAR", fullLen, null, false);
        builder.index(SCHEMA, TABLE, "v");
        builder.indexColumn(SCHEMA, TABLE, "v", "v", 0, true, indexedLength);
        builder.createGroup(TABLE, SCHEMA);
        builder.addTableToGroup(TABLE, SCHEMA, TABLE);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        return builder.akibanInformationSchema();
    }

    private static void validate(AkibanInformationSchema ais) {
        ais.validate(Collections.<AISValidation>singleton(new IndexColumnIsNotPartial())).throwIfNecessary();
    }

    @Test
    public void nullIndexedLengthIsValid() {
        validate(createAIS(32, null));
    }

    @Test(expected=IndexColumnIsPartialException.class)
    public void fullLengthIsInvalid() {
        validate(createAIS(32, 32));
    }

    @Test(expected=IndexColumnIsPartialException.class)
    public void partialLengthIsInvalid() {
        validate(createAIS(32, 16));
    }
}
