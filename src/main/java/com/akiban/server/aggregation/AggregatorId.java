/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.aggregation;

import com.akiban.server.types.AkType;
import com.akiban.util.ArgumentValidation;

public final class AggregatorId {

    public AggregatorId(String name, AkType type) {
        this.name = name.toUpperCase();
        this.type = type;
        ArgumentValidation.notNull("name", this.name);
        ArgumentValidation.notNull("type", this.type);
    }

    public String name() {
        return name;
    }

    public AkType type() {
        return type;
    }

    // object interface

    @Override
    public String toString() {
        return name + " -> " + type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregatorId that = (AggregatorId) o;
        return name.equals(that.name) && type == that.type;

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    private final String name;
    private final AkType type;
}
