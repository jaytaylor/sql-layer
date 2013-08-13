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

package com.akiban.server.entity.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

public final class EntityField extends EntityElement {

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = Collections.unmodifiableMap(new TreeMap<>(properties));
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Set<Validation> getValidations() {
        return validations;
    }

    public void setValidations(Set<Validation> validations) {
        this.validations = Collections.unmodifiableSet(new TreeSet<>(validations));
    }

    public static EntityField modifiableScalar(UUID uuid, String type) {
        EntityField field = new EntityField();
        field.setUuid(uuid);
        field.setType(type);
        field.properties = new TreeMap<>();
        field.validations = new TreeSet<>();
        return field;
    }

    private String type;
    private Map<String, Object> properties = Collections.emptyMap();
    private Set<Validation> validations = Collections.emptySet();
}
