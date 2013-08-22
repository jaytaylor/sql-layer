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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class EntityCollection extends Entity {

    @JsonProperty("grouping_fields")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getGroupingFields() {
        return parentColumns;
    }

    @JsonProperty("grouping_fields")
    public void setGroupingFields(List<String> groupingFields) {
        this.parentColumns = ImmutableList.copyOf(groupingFields);
    }

    @Override
    protected void acceptStart(EntityVisitor visitor) {
        visitor.enterCollection(this);
    }

    @Override
    protected void acceptFinish(EntityVisitor visitor) {
        visitor.leaveCollection();
    }

    public static EntityCollection modifiableCollection(UUID uuid) {
        EntityCollection collection = new EntityCollection();
        collection.makeModifiable(uuid);
        collection.parentColumns = new ArrayList<>();
        return collection;
    }

    private List<String> parentColumns = Collections.emptyList();
}
