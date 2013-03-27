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

import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import java.util.List;

public final class EntityIndex {

    public List<EntityColumn> getColumns() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntityIndex that = (EntityIndex) o;
        return columns.equals(that.columns);

    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    @Override
    public String toString() {
        return columns.toString();
    }

    @JsonValue
    public Object asJsonValue() {
        return columns;
    }

    @JsonCreator
    public EntityIndex(List<EntityColumn> columns) {
        this.columns = ImmutableList.copyOf(columns);
    }

    public static EntityIndex create(List<EntityColumn> columns) {
        return new EntityIndex(columns);
    }

    private final List<EntityColumn> columns;
}
