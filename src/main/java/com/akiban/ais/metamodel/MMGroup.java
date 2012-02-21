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
import com.akiban.ais.model.Group;
import com.akiban.ais.model.ModelNames;
import com.akiban.ais.model.validation.AISInvariants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MMGroup implements Serializable, ModelNames {
    public MMGroup(Group group) {
        name = group.getName();
    }

    public static Group create(AkibanInformationSchema ais, Map<String, Object> map)
    {
        return Group.create(ais, (String) map.get(group_name));
    }

    public Map<String, Object> map()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(group_name, name);
        return map;
    }

    private String name;
}
