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

final class EntityUtil {

    private EntityUtil() {}

    public static <T> T cast(Object o, Class<T> target) {
        if (o == null)
            throw new IllegalEntityDefinition("expected " + target.getSimpleName() + " but found null");
        try {
            return target.cast(o);
        } catch (ClassCastException e) {
            throw new IllegalEntityDefinition("expected " + target.getSimpleName()
                    + " but found found " + o.getClass().getSimpleName());
        }
    }
}
