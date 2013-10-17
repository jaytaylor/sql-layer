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

import java.util.HashMap;
import java.util.Map;

import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.API.SortOption;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.config.TestConfigService;
import com.foundationdb.util.tap.InOutTap;

public class MergeJoinSorterIT extends SorterITBase {

    @Override
    public Map<String,String> startupConfigProperties() {
        Map<String,String> props = new HashMap<>();
        props.putAll(super.startupConfigProperties());

        props.put("fdbsql.tmp_dir", TestConfigService.dataDirectory().getAbsolutePath());
        return props;
    }

    @Override
    public Sorter createSorter(QueryContext context, QueryBindings bindings,
            Cursor input, RowType rowType, Ordering ordering,
            SortOption sortOption, InOutTap loadTap) {
        return new MergeJoinSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
    }

}
