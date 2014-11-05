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

package com.foundationdb.server.store.format;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.server.types.service.TestTypesRegistry;

import java.util.HashSet;
import java.util.Set;

public class DummyStorageFormatRegistry extends StorageFormatRegistry
{
    private final Set<String> generated;
    private final static String identifier = "dummy";

    public DummyStorageFormatRegistry() {
        super("dummy");
        this.generated = new HashSet<>();
    }

    @Override
    public void registerStandardFormats() {
        TestStorageFormat.register(this);
        super.registerStandardFormats();
    }

    @Override
    void getDefaultDescriptionConstructor() {}

    @Override
    public StorageDescription getDefaultStorageDescription(HasStorage object) {
        return null;
    }

    @Override
    public boolean isDescriptionClassAllowed(Class<? extends StorageDescription> descriptionClass) {
        return true;
    }

    public static StorageFormatRegistry create() {
        StorageFormatRegistry result = new DummyStorageFormatRegistry();
        result.registerStandardFormats();
        return result;
    }

    /** Convenience to make an AISCloner using the dummy. */
    public static AISCloner aisCloner() {
        return new AISCloner(TestTypesRegistry.MCOMPAT, create());
    }
    
    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        super.finishStorageDescription(object, nameGenerator);
        if (object.getStorageDescription() == null) {
            object.setStorageDescription(new TestStorageDescription(object, generateStorageKey(object), identifier));
        }
        assert object.getStorageDescription() != null;
    }

    protected String generateStorageKey(HasStorage object) {
        final String proposed;
        if (object instanceof Index) {
            proposed = ((Index)object).getIndexName().toString();
        }
        else if (object instanceof Group) {
            proposed = ((Group)object).getName().toString();
        }
        else if (object instanceof Sequence) {
            proposed =  ((Sequence)object).getSequenceName().toString();
        }
        else {
            throw new IllegalArgumentException(object.toString());
        }
        return DefaultNameGenerator.makeUnique(generated, proposed, Integer.MAX_VALUE);
    }
}
