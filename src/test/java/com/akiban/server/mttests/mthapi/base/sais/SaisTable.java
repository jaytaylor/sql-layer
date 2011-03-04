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

package com.akiban.server.mttests.mthapi.base.sais;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public final class SaisTable {
    private final String name;
    private final Set<String> fields;
    private final Set<SaisFK> children;

    SaisTable(String name, Set<String> fields, Set<SaisFK> children) {
        this.name = name;
        this.fields = Collections.unmodifiableSet(fields);
        this.children = children;
    }

    public SaisTable getChild(String name) {
        for (SaisFK fk : getChildren()) {
            SaisTable child = fk.getChild();
            if (child.getName().equals(name)) {
                return child;
            }
        }
        throw new NoSuchElementException(name);
    }

    public Set<SaisFK> getChildren() {
        return children;
    }

    public String getName() {
        return name;
    }

    public Set<String> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return buildString(new StringBuilder()).toString();
    }

    private StringBuilder buildString(StringBuilder builder) {
        builder.append(name).append(fields);
        if (getChildren().isEmpty()) {
            return builder;
        }
        Iterator<SaisFK> fkIterator = getChildren().iterator();
        builder.append(" -> ( ");
        while (fkIterator.hasNext()) {
            SaisFK fk = fkIterator.next();
            builder.append("COLS").append(fk.getFkFields()).append(" REFERENCE ");
            fk.getChild().buildString(builder);
            if (fkIterator.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append(" ) ");
        return builder;
    }
}
