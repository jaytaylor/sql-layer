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
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.ColumnSizeMismatchException;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.util.JUnitUtils;
import org.junit.Test;

import java.util.Collections;

public class ColumnMaxAndPrefixSizesMatchTest {
    private final static String SCHEMA = "test";
    private final static String TABLE = "t";

    private static AkibanInformationSchema createAIS(Long maxStorage, Integer prefix) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        Table table = Table.create(ais, SCHEMA, TABLE, 1);
        Column.create(table, "id", 0, TestTypesRegistry.MCOMPAT.getTypeClass("MCOMPAT", "BIGINT").instance(false), maxStorage, prefix);
        return ais;
    }

    private static void validate(AkibanInformationSchema ais) {
        ais.validate(Collections.<AISValidation>singleton(new ColumnMaxAndPrefixSizesMatch())).throwIfNecessary();
    }

    @Test
    public void nulls() {
        validate(createAIS(null, null));
    }

    @Test
    public void correct() {
        validate(createAIS(8L, 0));
    }

    @Test(expected=ColumnSizeMismatchException.class)
    public void wroteMaxIsError() {
        validate(createAIS(50L, 0));
    }

    @Test(expected=ColumnSizeMismatchException.class)
    public void wrongPrefixIsError() {
        validate(createAIS(8L, 50));
    }

    @Test
    public void wrongStorageAndPrefixIsError() {
        JUnitUtils.expectMultipleCause(
            new Runnable() {
                @Override
                public void run() {
                    validate(createAIS(50L, 50));
                }
            },
            ColumnSizeMismatchException.class, // maxStorage
            ColumnSizeMismatchException.class  // prefix
        );
    }
}
