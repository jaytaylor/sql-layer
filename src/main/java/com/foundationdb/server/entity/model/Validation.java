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

import com.foundationdb.util.ArgumentValidation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class Validation implements Comparable<Validation> {

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Validation that = (Validation) o;
        return name.equals(that.name) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", name, value);
    }

    @Override
    public int compareTo(Validation o) {
        int keyCompares = name.compareTo(o.name);
        if (keyCompares != 0)
            return keyCompares;
        if (value == null)
            return o.value == null ? 0 : -1;
        else if (o.value == null)
            return 1;
        String valueString = value.toString();
        String oValueString = o.value.toString();
        return valueString.compareTo(oValueString);
    }

    @JsonValue
    public Object asJsonValue() {
        return Collections.singletonMap(name, value);
    }

    @JsonCreator
    Validation(Map<String, ?> validation) {
        if (validation.size() != 1)
            throw new IllegalEntityDefinition("illegal validation definition (map must have one entry)");
        Map.Entry<String, ?> entry = validation.entrySet().iterator().next();
        this.name = entry.getKey();
        this.value = entry.getValue();
    }

    // for testing
    public Validation(String name, Object value) {
        ArgumentValidation.notNull("validation name", name);
        this.name = name;
        this.value = value;
    }

    private final String name;
    private final Object value;
}
