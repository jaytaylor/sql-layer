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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;


import java.util.Comparator;
import java.util.UUID;

public abstract class EntityElement {

    public UUID getUuid() {
        return uuid;
    }

    @JsonProperty("uuid")
    public void setUuid(String string) {
        UUID uuid;
        try {
            uuid = string == null ? null : UUID.fromString(string);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalEntityDefinition("invalid uuid");
        }
        setUuid(uuid);
    }

    @JsonIgnore
    void setUuid(UUID uuid) {
        if (this.uuid != null)
            throw new IllegalStateException("uuid already set: " + this.uuid);
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", getName(), getUuid());
    }

    private UUID uuid;
    private String name;

    public static final Comparator<? super EntityElement> byName = new Comparator<EntityElement>() {
        @Override
        public int compare(EntityElement o1, EntityElement o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };

    public static final Function<? super EntityElement, String> toName = new Function<EntityElement, String>() {
        @Override
        public String apply(EntityElement input) {
            return input == null ? null : input.getName();
        }
    };

    public static final Function<? super EntityElement, ? extends UUID> toUuid = new Function<EntityElement, UUID>() {
        @Override
        public UUID apply(EntityElement input) {
            return input == null ? null : input.getUuid();
        }
    };

    public static boolean sameByUuid(EntityElement one, EntityElement two) {
        if (one == null)
            return two == null;
        return two != null && one.getUuid().equals(Preconditions.checkNotNull(two.getUuid()));
    }
}
