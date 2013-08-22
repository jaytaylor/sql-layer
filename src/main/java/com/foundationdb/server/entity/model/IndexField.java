/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class IndexField {

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @JsonValue
    public abstract Object toJsonValue();

    @JsonCreator
    public static IndexField create(Object def) {
        if (def instanceof IndexField) {
            return (IndexField) def;
        }
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FieldName other = (FieldName) o;
            return Objects.equals(fieldName, other.fieldName);
        }

        @Override
        public int hashCode() {
            return fieldName != null ? fieldName.hashCode() : 0;
        }

        @Override
        public String toString() {
            return getFieldName();
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            QualifiedFieldName other = (QualifiedFieldName) o;
            return Objects.equals(entityName, other.entityName)
                    && Objects.equals(getFieldName(), other.getFieldName());
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (entityName != null ? entityName.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return String.format("%s.%s", getEntityName(), getFieldName());
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
                    Arrays.asList(latitude.toJsonValue(), longitude.toJsonValue()));
        }

        public IndexField getLatitude() {
            return latitude;
        }

        public IndexField getLongitude() {
            return longitude;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SpatialField other = (SpatialField) o;
            return Objects.equals(longitude, other.longitude) && Objects.equals(latitude, other.latitude);
        }

        @Override
        public int hashCode() {
            int result = latitude != null ? latitude.hashCode() : 0;
            result = 31 * result + (longitude != null ? longitude.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return String.format("lat_lon(%s, %s)", latitude, longitude);
        }

        public SpatialField(FieldName lat, FieldName lon) {
            this.latitude = lat;
            this.longitude = lon;
        }

        private final FieldName latitude;
        private final FieldName longitude;
    }
}
