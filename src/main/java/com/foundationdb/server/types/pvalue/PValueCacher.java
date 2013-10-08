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

package com.foundationdb.server.types.pvalue;

import com.foundationdb.server.types.TInstance;

public interface PValueCacher {
    void cacheToValue(Object cached, TInstance tInstance, PBasicValueTarget target);
    Object valueToCache(PBasicValueSource value, TInstance tInstance);
    Object sanitize(Object object);
    boolean canConvertToValue(Object cached);
}
