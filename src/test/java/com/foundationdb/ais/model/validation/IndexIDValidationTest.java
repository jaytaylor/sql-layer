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
import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Index.JoinType;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.server.error.DuplicateIndexIdException;
import com.foundationdb.server.error.InvalidIndexIDException;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import org.junit.Test;

import java.util.Collections;

public class IndexIDValidationTest
{
    private static void validate(AkibanInformationSchema ais) {
        ais.validate(Collections.singleton(new IndexIDValidation())).throwIfNecessary();
    }

    private final TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;

    @Test(expected=DuplicateIndexIdException.class)
    public void dupSameTable() {
        AkibanInformationSchema ais = AISBBasedBuilder
            .create("test", typesTranslator)
            .table("p").colInt("id").key("k1", "id").key("k2", "id")
            .unvalidatedAIS();
        ais.getTable("test", "p").getIndex("k1").setIndexId(10);
        ais.getTable("test", "p").getIndex("k2").setIndexId(10);
        validate(ais);
    }

    @Test(expected=DuplicateIndexIdException.class)
    public void dupDifferentTable() {
        AkibanInformationSchema ais = AISBBasedBuilder
            .create("test", typesTranslator)
            .table("p").colInt("id").pk("id").key("k1", "id")
            .table("c").colInt("id").colInt("pid").key("k2", "id").joinTo("p").on("pid", "id")
            .unvalidatedAIS();
        ais.getTable("test", "p").getIndex("k1").setIndexId(10);
        ais.getTable("test", "c").getIndex("k2").setIndexId(10);
        validate(ais);
    }

    @Test(expected=DuplicateIndexIdException.class)
    public void dupTableAndFullText() {
        AkibanInformationSchema ais = AISBBasedBuilder
            .create("test", typesTranslator)
            .table("p").colInt("id").colString("s", 255).key("k1")
            .unvalidatedAIS();
        Table t = ais.getTable("test", "p");
        t.getIndex("k1").setIndexId(10);
        Index idx = FullTextIndex.create(ais, t, "k2", 10);
        IndexColumn.create(idx, t.getColumn("s"), 0, true, null);
        validate(ais);
    }

    @Test(expected=DuplicateIndexIdException.class)
    public void dupTableAndGroup() {
        AkibanInformationSchema ais = AISBBasedBuilder
            .create("test", typesTranslator)
            .table("p").colInt("id").pk("id").key("k1", "id")
            .table("c").colInt("id").colInt("pid").joinTo("p").on("pid", "id")
            .groupIndex("k2", JoinType.LEFT).on("c", "id").and("p", "id")
            .unvalidatedAIS();
        ais.getTable("test", "p").getIndex("k1").setIndexId(10);
        ais.getTable("test", "p").getGroup().getIndex("k2").setIndexId(10);
        validate(ais);
    }

    @Test(expected=InvalidIndexIDException.class)
    public void nullID() {
        AkibanInformationSchema ais = AISBBasedBuilder
            .create("test", typesTranslator)
            .table("p").colInt("id").key("k", "id")
            .unvalidatedAIS();
        ais.getTable("test", "p").getIndex("k").setIndexId(null);
        validate(ais);
    }

    @Test(expected=InvalidIndexIDException.class)
    public void negativeID() {
        AkibanInformationSchema ais = AISBBasedBuilder
            .create("test", typesTranslator)
            .table("p").colInt("id").key("k", "id")
            .unvalidatedAIS();
        ais.getTable("test", "p").getIndex("k").setIndexId(-1);
        validate(ais);
    }

    @Test(expected=InvalidIndexIDException.class)
    public void zeroID() {
        AkibanInformationSchema ais = AISBBasedBuilder
            .create("test", typesTranslator)
            .table("p").colInt("id").key("k", "id")
            .unvalidatedAIS();
        ais.getTable("test", "p").getIndex("k").setIndexId(0);
        validate(ais);
    }
}
