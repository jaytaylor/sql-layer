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
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class OrdinalOrderingTest
{
    private static Collection<AISValidationFailure> validate(AkibanInformationSchema ais) {
        return ais.validate(Collections.singleton(new OrdinalOrdering())).failures();
    }


    private final TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;

    @Test
    public void noOrdinal() {
        AkibanInformationSchema ais = AISBBasedBuilder
            .create("test", typesTranslator)
            .table("p").colInt("id")
            .unvalidatedAIS();
        ais.getTable("test", "p").setOrdinal(null);
        Collection<AISValidationFailure> failures = validate(ais);
        assertEquals("failures", 1, failures.size());
    }

    @Test
    public void lowerOrdinal() {
        AkibanInformationSchema ais = AISBBasedBuilder
            .create("test", typesTranslator)
            .table("p").colInt("pid").pk("pid")
            .table("c").colInt("cid").colInt("pid").pk("cid").joinTo("p").on("pid", "pid")
            .unvalidatedAIS();
        ais.getTable("test", "p").setOrdinal(2);
        ais.getTable("test", "c").setOrdinal(1);
        Collection<AISValidationFailure> failures = validate(ais);
        assertEquals("failures", 1, failures.size());
    }
}
