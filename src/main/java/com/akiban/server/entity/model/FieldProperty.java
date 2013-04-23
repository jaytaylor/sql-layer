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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Sequence;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;

public abstract class FieldProperty {
    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @JsonValue
    public abstract Object toJsonValue();


    public static class IdentityProperty extends FieldProperty {
        public static final String PROPERTY_NAME = "identity";

        private final boolean isDefault;
        private final long start;
        private final long increment;

        public static IdentityProperty create(Object o) {
            if(o instanceof Column) {
                Column column = (Column)o;
                Boolean isDefault = column.getDefaultIdentity();
                if(isDefault == null) {
                    throw new IllegalArgumentException("Column not identity: " + column);
                }
                Sequence seq = column.getIdentityGenerator();
                return new IdentityProperty(isDefault,
                                            seq.getStartsWith(),
                                            seq.getIncrement());
            } else if(o instanceof Map) {
                Map map = (Map)o;
                return new IdentityProperty(EntityUtil.cast(map.get("default"), Boolean.class),
                                            EntityUtil.cast(map.get("start"), Number.class),
                                            EntityUtil.cast(map.get("increment"), Number.class));
            }
            throw new IllegalArgumentException("Can't create from: " + o);
        }

        public IdentityProperty(boolean isDefault, Number start, Number increment) {
            this.isDefault = isDefault;
            this.start = start.longValue();
            this.increment = increment.longValue();
        }

        public boolean isDefault() {
            return isDefault;
        }

        public long getStart() {
            return start;
        }

        public long getIncrement() {
            return increment;
        }

        @Override
        public boolean equals(Object o) {
            if(this == o) {
                return true;
            }
            if(o == null || getClass() != o.getClass()) {
                return false;
            }
            IdentityProperty rhs = (IdentityProperty)o;
            return isDefault == rhs.isDefault &&
                   start == rhs.start &&
                   increment == rhs.increment;

        }

        @Override
        public int hashCode() {
            int result = (isDefault ? 1 : 0);
            result = 31 * result + (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (increment ^ (increment >>> 32));
            return result;
        }

        @Override
        @JsonValue
        public Object toJsonValue() {
            Map<String,Object> map = new HashMap<>();
            map.put("default", isDefault);
            map.put("start", start);
            map.put("increment", increment);
            return map;
        }
    }
}
