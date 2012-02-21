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
import com.akiban.ais.model.ModelNames;
import com.akiban.ais.model.Type;
import com.akiban.server.types.AkType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MMType implements Serializable, ModelNames {
    public MMType(Type type) {
        name = type.name();
        typeParameters = type.nTypeParameters();
        fixedSize = type.fixedSize();
        maxStorageSizeBytes = type.maxSizeBytes();
        encoding = type.encoding();
        akType = type.akType();
    }

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

    public Map<String, Object> map()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(type_name, name);
        map.put(type_parameters, typeParameters);
        map.put(type_fixedSize, fixedSize);
        map.put(type_maxSizeBytes, maxStorageSizeBytes);
        map.put(type_encoding, encoding);
        return map;
    }

    private String name;
    private Integer typeParameters;
    private Boolean fixedSize;
    private Long maxStorageSizeBytes;
    private String encoding;
    private AkType akType;
}
