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

package com.foundationdb.server.test.it.sort;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.storeadapter.indexcursor.PersistitSorter;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager.BindingsConfigurationProvider;
import com.foundationdb.server.test.it.PersistitITBase;
import com.foundationdb.util.tap.InOutTap;

import java.util.Map;

public final class PersistitSorterIT extends SorterITBase
{
    protected BindingsConfigurationProvider serviceBindingsProvider() {
        return PersistitITBase.doBind(super.serviceBindingsProvider());
    }

    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Override
    public Sorter createSorter(QueryContext context,
                               QueryBindings bindings,
                               Cursor input,
                               RowType rowType,
                               API.Ordering ordering,
                               API.SortOption sortOption,
                               InOutTap loadTap) {
        return new PersistitSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
    }
}
