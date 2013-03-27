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

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class EntityIndex {

    public List<IndexField> getFields() {
        return fields;
    }

    public void setFields(List<IndexField> fields) {
        this.fields = fields;
    }

    public IndexType getType() {
        return type;
    }

    public void setType(IndexType type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntityIndex that = (EntityIndex) o;
        return fields.equals(that.fields);

    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }

    @Override
    public String toString() {
        return fields.toString();
    }

    @JsonValue
    public Object asJsonValue() {
        if (type == null)
            return fields;
        Map<String, Object> asMap = new HashMap<>();
        asMap.put("fields", fields);
        asMap.put("type", type.name().toLowerCase());
        return fields;
    }

    @JsonCreator
    public static EntityIndex create(Object def) {
        EntityIndex result = new EntityIndex();
        if (def instanceof List) {
            createIndexFields(def, result);
        }
        else if (def instanceof Map) {
            Map<?,?> asMap = cast(def, Map.class);
            Object fieldObj = asMap.get("fields");
            String typeObj = cast(asMap.get("type"), String.class);
            createIndexFields(fieldObj, result);
            result.type = IndexType.valueOf(typeObj.toUpperCase());
        }
        else {
            throw new IllegalEntityDefinition("expected index definition");
        }
        return result;
    }

    private static void createIndexFields(Object def, EntityIndex result) {
        List<?> defList = cast(def, List.class);
        result.fields = new ArrayList<>(defList.size());
        for (Object field : defList)
            result.fields.add(IndexField.create(field));
    }

    private static <T> T cast(Object o, Class<T> target) {
        if (o == null)
            throw new IllegalEntityDefinition("expected " + target.getSimpleName() + " but found null");
        try {
            return target.cast(o);
        } catch (ClassCastException e) {
            throw new IllegalEntityDefinition("expected " + target.getSimpleName()
                    + " but found found " + o.getClass().getSimpleName());
        }
    }

    public EntityIndex() {
    }

    private List<IndexField> fields;
    private IndexType type;

    public enum IndexType {
        LEFT, RIGHT, FULL_TEXT
    }
}
