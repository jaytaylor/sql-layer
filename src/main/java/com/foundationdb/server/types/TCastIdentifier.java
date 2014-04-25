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

import com.google.common.base.Objects;

public final class TCastIdentifier {

    public TClass getSource() {
        return source;
    }

    public TClass getTarget() {
        return target;
    }

    @Override
    public String toString() {
        return source + " to " + target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TCastIdentifier that = (TCastIdentifier) o;

        return Objects.equal(this.source, that.source) && Objects.equal(this.target, that.target);
    }

    @Override
    public int hashCode() {
        int result = source != null ? source.hashCode() : 0;
        result = 31 * result + (target != null ? target.hashCode() : 0);
        return result;
    }

    public TCastIdentifier(TClass source, TClass target) {
        this.source = source;
        this.target = target;
    }

    public TCastIdentifier(TCast cast) {
        this(cast.sourceClass(), cast.targetClass());
    }

    private final TClass source;
    private final TClass target;
}
