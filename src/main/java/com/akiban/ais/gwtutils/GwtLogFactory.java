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

package com.akiban.ais.gwtutils;

public interface GwtLogFactory
{
    /**
     * Get a logger for the appropriate class. Users should not make any assumption about this logger, other than
     * that it will not throw any exceptions. In particular, they shouldn't make assumptions about its identity;
     * if you want to return the same logger for each class, that's fine.
     * @param clazz the class for which we want logging
     * @return the logger
     */
    GwtLogger getLogger(Class<?> clazz);
}
