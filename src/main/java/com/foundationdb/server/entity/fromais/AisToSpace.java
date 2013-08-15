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

package com.foundationdb.server.entity.fromais;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.server.entity.model.Entity;
import com.foundationdb.server.entity.model.Space;
import com.google.common.base.Function;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class AisToSpace {

    public static Space create(AkibanInformationSchema ais, Function<String, UUID> uuidGenerator) {
        Collection<Entity> entities = new ArrayList<>(ais.getGroups().size());
        for (Group group : ais.getGroups().values()) {
            UserTable root = group.getRoot();
            Entity entity = new EntityBuilder(root).getEntity();
            entities.add(entity);
        }
        return Space.create(entities, uuidGenerator);
    }

    private AisToSpace() {}
}
