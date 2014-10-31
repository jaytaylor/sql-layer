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

import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;

abstract class ClientAPIBase {

    private final Store store;
    private final SchemaManager schemaManager;
    private final BasicDXLMiddleman middleman;

    ClientAPIBase(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store) {
        this.middleman = middleman;
        this.schemaManager = schemaManager;
        this.store = store;
    }

    final public Store store() {
        return store;
    }

    final public SchemaManager schemaManager() {
        return schemaManager;
    }

    BasicDXLMiddleman middleman() {
        return middleman;
    }
}
