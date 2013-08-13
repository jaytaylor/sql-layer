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

package com.foundationdb.server.entity.model;

import com.foundationdb.ais.model.Index;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class EntityIndex {

    public List<IndexField> getFields() {
        return fields;
    }

    void setFields(List<IndexField> fields) {
        this.fields = ImmutableList.copyOf(fields);
    }

    public IndexType getType() {
        return type;
    }

    void setType(IndexType type) {
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
        EntityIndex result;
        if (def instanceof List) {
            result = createIndexFields(def);
        }
        else if (def instanceof Map) {
            Map<?,?> asMap = EntityUtil.cast(def, Map.class);
            Object fieldObj = asMap.get("fields");
            String typeObj = EntityUtil.cast(asMap.get("type"), String.class);
            result = createIndexFields(fieldObj);
            result.type = IndexType.valueOf(typeObj.toUpperCase());
        }
        else {
            throw new IllegalEntityDefinition("expected index definition");
        }
        return result;
    }

    private static EntityIndex createIndexFields(Object def) {
        List<?> defList = EntityUtil.cast(def, List.class);
        List<IndexField> fields = new ArrayList<>(defList.size());
        for (Object field : defList)
            fields.add(IndexField.create(field));
        return new EntityIndex(fields, null);
    }

    public EntityIndex(List<IndexField> fields, IndexType type) {
        this.fields = ImmutableList.copyOf(fields);
        this.type = type;
    }

    private ImmutableList<IndexField> fields;
    private IndexType type;

    public enum IndexType {
        LEFT, RIGHT, FULL_TEXT;

        public static IndexType valueOf(Index.JoinType aisJoinType) {
            if (aisJoinType == null)
                return null;
            switch (aisJoinType) {
            case LEFT:  return LEFT;
            case RIGHT: return RIGHT;
            default: throw new AssertionError(aisJoinType);
            }
        }
    }
}
