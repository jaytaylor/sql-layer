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

public final class TInstanceBuilder {

    public TInstanceBuilder setNullable(boolean nullable) {
        if (workingCopy != null) {
            if (workingCopy.nullability() == nullable)
                return this;
            copyFromWorking();
        }
        this.nullable = nullable;
        return this;
    }

    public TInstanceBuilder setAttribute(Attribute attribute, int value) {
        if (workingCopy != null) {
            if (workingCopy.attribute(attribute) == value)
                return this;
            copyFromWorking();
        }
        switch (attribute.ordinal()) {
        case 0:
            attr0 = value;
            break;
        case 1:
            attr1 = value;
            break;
        case 2:
            attr2 = value;
            break;
        case 3:
            attr3 = value;
            break;
        }
        return this;
    }

    public TInstanceBuilder copyFrom(TInstance type) {
        if (type.typeClass() != orig.typeClass() || type.enumClass() != orig.enumClass())
            throw new IllegalArgumentException("can't copy " + type + " to a builder based on " + orig);
        this.workingCopy = type;
        return this;
    }

    public TInstance get() {
        if (workingCopy == null) // all of our mutations were noops, so we can just return the old TInstance
            workingCopy = TInstance.create(orig, attr0, attr1, attr2, attr3, nullable);
        return workingCopy;
    }

    public TInstanceBuilder(TInstance orig) {
        this.orig = orig;
        this.workingCopy = orig;
        this.nullable = orig.nullability();
    }

    private void copyFromWorking() {
        assert nullable == workingCopy.nullability();
        attr0 = workingCopy.attrByPos(0);
        attr1 = workingCopy.attrByPos(1);
        attr2 = workingCopy.attrByPos(2);
        attr3 = workingCopy.attrByPos(3);
        workingCopy = null;
    }

    private final TInstance orig;
    private int attr0;
    private int attr1;
    private int attr2;
    private int attr3;
    private boolean nullable;
    private TInstance workingCopy;
}
