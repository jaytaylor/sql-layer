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

package com.foundationdb.server.service.dxl;

import java.util.concurrent.atomic.AtomicReference;

public final class BasicDXLMiddleman {
    static BasicDXLMiddleman create() {
        BasicDXLMiddleman instance = new BasicDXLMiddleman();
        if (!lastInstance.compareAndSet(null, instance)) {
            throw new RuntimeException("there is already a BasicDXLMiddleman instance");
        }
        return instance;
    }

    static void destroy() {
        lastInstance.set(null);
    }

    static BasicDXLMiddleman last() {
        return lastInstance.get();
    }

    private BasicDXLMiddleman() {
    }

    private static final AtomicReference<BasicDXLMiddleman> lastInstance = new AtomicReference<>();
}
