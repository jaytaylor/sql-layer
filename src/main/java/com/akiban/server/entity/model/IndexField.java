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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IndexField {

    @JsonValue
    public Object asJsonValue() {
        throw new UnsupportedOperationException();
    }

    @JsonCreator
    public static IndexField create(Object def) {
        if (def instanceof Map) {
            Map<?,?> asMap = EntityUtil.cast(def, Map.class);
            if (asMap.size() != 1)
                throw new IllegalEntityDefinition("illegal index field:" + def);
            if (!asMap.containsKey("spatial"))
                throw new IllegalEntityDefinition("illegal index field:" + def);
            List<?> latLon = EntityUtil.cast(asMap.get("spatial"), List.class);
            if (latLon.size() != 2)
                throw new IllegalEntityDefinition("illegal spatial definition: " + latLon);
            FieldName lat = createFieldName(latLon.get(0));
            FieldName lon = createFieldName(latLon.get(1));
            return new SpatialField(lat, lon);
        }
        else {
            return createFieldName(def);
        }
    }

    private static FieldName createFieldName(Object def) {
        if (def instanceof String) {
            return new FieldName((String)def);
        }
        else if (def instanceof List) {
            List<?> asList = EntityUtil.cast(def, List.class);
            if (asList.size() != 2)
                throw new IllegalEntityDefinition("index field list must have exactly two elements");
            String entityName = EntityUtil.cast(asList.get(0), String.class);
            String fieldName = EntityUtil.cast(asList.get(1), String.class);
            return new QualifiedFieldName(entityName, fieldName);
        }
        else {
            throw new IllegalEntityDefinition("illegal index field: " + def);
        }
    }

    private IndexField() {}

    public static class FieldName extends IndexField {

        @JsonValue
        public Object toJsonValue() {
            return getFieldName();
        }

        public String getFieldName() {
            return fieldName;
        }

        public FieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        private final String fieldName;
    }

    public static class QualifiedFieldName extends FieldName {

        @Override
        @JsonValue
        public Object toJsonValue() {
            return Arrays.asList(getEntityName(), getFieldName());
        }

        public String getEntityName() {
            return entityName;
        }

        public QualifiedFieldName(String entityName, String fieldName) {
            super(fieldName);
            this.entityName = entityName;
        }

        private final String entityName;
    }

    public static class SpatialField extends IndexField {

        @JsonValue
        public Object toJsonValue() {
            return Collections.singletonMap(
                    "spatial",
                    Arrays.asList(latitude.asJsonValue(), longitude.asJsonValue()));
        }

        public IndexField getLatitude() {
            return latitude;
        }

        public IndexField getLongitude() {
            return longitude;
        }

        public SpatialField(FieldName lat, FieldName lon) {
            this.latitude = lat;
            this.longitude = lon;
        }

        private final FieldName latitude;
        private final FieldName longitude;
    }
}
