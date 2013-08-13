/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
import com.foundationdb.ais.model.Type;
import com.foundationdb.ais.model.Types;
import com.foundationdb.server.error.TypesAreStaticException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TypesAreFromStatic implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Type type : ais.getTypes()) {
            String typeName = type.name();
            Type fromStatic = staticTypesByName.get(typeName);
            if (type != fromStatic) {
                output.reportFailure(
                        new AISValidationFailure(
                                new TypesAreStaticException (type)));
            }
        }
    }

    private static Map<String,Type> staticTypesByName() {
        Map<String,Type> results = new HashMap<>();
        for (Type type : Types.types()) {
            results.put(type.name(), type);
        }
        return Collections.unmodifiableMap(results);
    }

    private final static Map<String,Type> staticTypesByName = staticTypesByName();
}
