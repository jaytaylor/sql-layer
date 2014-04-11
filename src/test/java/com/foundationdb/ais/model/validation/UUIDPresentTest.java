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
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class UUIDPresentTest
{
    private static Collection<AISValidationFailure> validate(AkibanInformationSchema ais) {
        return ais.validate(Collections.singleton(new UUIDPresent())).failures();
    }

    private static AkibanInformationSchema build() {
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        return AISBBasedBuilder.create("test", typesTranslator).table("t").colInt("id").pk("id").unvalidatedAIS();
    }

    @Test
    public void missingFromTable() {
        AkibanInformationSchema ais = build();
        Table t = ais.getTable("test", "t");
        t.setUuid(null);
        t.getColumn("id").setUuid(UUID.randomUUID());
        assertEquals("failures", 1, validate(ais).size());
    }

    @Test
    public void missingFromColumn() {
        AkibanInformationSchema ais = build();
        Table t = ais.getTable("test", "t");
        t.setUuid(UUID.randomUUID());
        t.getColumn("id").setUuid(null);
        assertEquals("failures", 1, validate(ais).size());
    }
}
