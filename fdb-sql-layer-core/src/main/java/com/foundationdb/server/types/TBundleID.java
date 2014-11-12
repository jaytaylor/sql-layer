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

package com.foundationdb.server.types;

import com.foundationdb.util.ArgumentValidation;

import java.util.UUID;

public final class TBundleID {

    public String name() {
        return name;
    }

    public UUID uuid() {
        return uuid;
    }

    // object interface

    @Override
    public int hashCode() {
        return uuid().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (obj.getClass() != TBundleID.class)
            return false;
        TBundleID other = (TBundleID) obj;
        return uuid.equals(other.uuid);
    }

    @Override
    public String toString() {
        return name;
    }

    public TBundleID(String name, String uuid) {
        this(name, UUID.fromString(uuid));
    }

    public TBundleID(String name, UUID uuid) {
        ArgumentValidation.notNull("name", name);
        ArgumentValidation.notNull("uuid", uuid);
        if (!name.matches(VALID_NAME))
            throw new IllegalNameException("name must contain only letters: " + name);
        this.name = name.toUpperCase();
        this.uuid = uuid;
    }

    private final String name;
    private final UUID uuid;
    private static final String VALID_NAME = "[a-zA-Z]+";
}
