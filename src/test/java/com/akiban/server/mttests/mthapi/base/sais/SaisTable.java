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

import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public final class SaisTable {
    private final String name;
    private final List<String> fields;
    private final List<String> pk;
    private final Set<SaisFK> children;
    private final AtomicReference<SaisFK> parentFK;

    SaisTable(String name, List<String> fields, List<String> pk, Set<SaisFK> children) {
        if (!children.isEmpty()) {
            if (pk == null) {
                throw new IllegalArgumentException("can't join to " + name + "; it has no PK");
            }
            for (SaisFK child : children) {
                List<String> childParentCols = child.getParentColsList();
                if (!pk.equals(childParentCols)) {
                    throw new IllegalArgumentException(String.format(
                            "wrong AkibanFK cols: %s to PK %s", childParentCols, pk
                    ));
                }
            }
        }
        Set<String> fieldsSet = new HashSet<String>(fields);
        if (fieldsSet.size() != fields.size()) {
            throw new IllegalArgumentException("some duplicated columns: " + fields);
        }
        this.name = name;
        this.fields = Collections.unmodifiableList(new ArrayList<String>(fields));
        this.pk = (pk == null) ? null : Collections.unmodifiableList(new ArrayList<String>(pk));
        this.parentFK = new AtomicReference<SaisFK>(null);
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

    void setParentFK(SaisFK parent) {
        ArgumentValidation.notNull("parent", parent);
        if (parent.getChild() != this) {
            throw new IllegalArgumentException(parent + " doesn't point to me! I'm: " + this);
        }
        if (!parentFK.compareAndSet(null, parent)) {
            throw new IllegalStateException("can't set ParentFK twice");
        }
    }

    public SaisFK getParentFK() {
        return parentFK.get();
    }

    public String getName() {
        return name;
    }

    public List<String> getFields() {
        return fields;
    }

    public List<String> getPK() {
        return pk;
    }

    public int countIncludingChildren() {
        int count = 1; // 1 for this
        for (SaisFK childFK : getChildren()) {
            count += childFK.getChild().countIncludingChildren();
        }
        return count;
    }

    @Override
    public String toString() {
        return getName();
    }

    public Set<SaisTable> setIncludingChildren() {
        Set<SaisTable> out = new HashSet<SaisTable>();
        buildSetIncludingChildren(this, out);
        return out;
    }

    @SuppressWarnings("unused")
    public static Set<SaisTable> setIncludingChildren(Set<SaisTable> roots) {
        Set<SaisTable> out = new HashSet<SaisTable>();
        for (SaisTable root : roots) {
            buildSetIncludingChildren(root, out);
        }
        return out;
    }

    private static void buildSetIncludingChildren(SaisTable root, Set<SaisTable> out) {
        boolean addedOut = out.add(root);
        assert addedOut : String.format("%s already in %s", root, out);
        for (SaisFK childFK : root.getChildren()) {
            buildSetIncludingChildren(childFK.getChild(), out);
        }
    }

    @SuppressWarnings("unused")
    public StringBuilder buildString(StringBuilder builder) {
        builder.append(name).append(fields);
        if (getChildren().isEmpty()) {
            return builder;
        }
        Iterator<SaisFK> fkIterator = getChildren().iterator();
        builder.append(" -> ( ");
        while (fkIterator.hasNext()) {
            SaisFK fk = fkIterator.next();
            builder.append("COLS").append(fk.getFkPairs()).append(" REFERENCE ");
            fk.getChild().buildString(builder);
            if (fkIterator.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append(" ) ");
        return builder;
    }
}
