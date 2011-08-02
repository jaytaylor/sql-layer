/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
import com.akiban.message.ErrorCode;

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
                        new AISValidationFailure(ErrorCode.INTERNAL_ERROR, "%s isn't from the static list", type)
                );
            }
        }
    }

    private Map<String,Type> staticTypesByName() {
        Map<String,Type> results = new HashMap<String, Type>();
        for (Type type : Types.types()) {
            results.put(type.name(), type);
        }
        return Collections.unmodifiableMap(results);
    }

    private final Map<String,Type> staticTypesByName = staticTypesByName();
}
