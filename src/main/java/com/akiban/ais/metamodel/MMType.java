/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.akiban.ais.metamodel;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Type;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MMType implements Serializable, ModelNames {
    public static Type create(AkibanInformationSchema ais, Map<String, Object> map)
    {
        return Type.create(ais,
                           (String) map.get(type_name),
                           (Integer) map.get(type_parameters),
                           (Boolean) map.get(type_fixedSize),
                           (Long) map.get(type_maxSizeBytes),
                           (String) map.get(type_encoding),
                           null);
    }

    public static Map<String, Object> map(Type type)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(type_name, type.name());
        map.put(type_parameters, type.nTypeParameters());
        map.put(type_fixedSize, type.fixedSize());
        map.put(type_maxSizeBytes, type.maxSizeBytes());
        map.put(type_encoding, type.encoding());
        return map;
    }
}
