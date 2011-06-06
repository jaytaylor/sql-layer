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

package com.akiban.server.service.servicemanager;

public interface ServiceLifecycleActions<T> {
    void onStart(T service) throws Exception;
    void onShutdown(T service) throws Exception;

    /**
     * Cast the given object to the actionable type if possible, or return {@code null} otherwise.
     * @param object the object which may or may not be actionable
     * @return the object reference, correctly casted; or null
     */
    T castIfActionable(Object object);
}
