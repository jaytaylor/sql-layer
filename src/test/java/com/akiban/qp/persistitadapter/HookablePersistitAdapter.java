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

package com.akiban.qp.persistitadapter;

import com.akiban.qp.rowtype.Schema;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.PersistitStore;

public final class HookablePersistitAdapter extends PersistitAdapter {

    public interface FilterFactoryHook extends PersistitFilterFactory.InternalHook {
        // empty interface; just promoting visibility
    }

    public HookablePersistitAdapter(Schema schema, PersistitStore persistit, Session session, TreeService treeService, FilterFactoryHook hook) {
        super(schema, persistit, session, treeService, null, hook);
    }
}
