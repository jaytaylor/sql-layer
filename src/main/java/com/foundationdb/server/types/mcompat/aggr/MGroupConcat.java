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

package com.foundationdb.server.types.mcompat.aggr;

import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TFixedTypeAggregator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

public class MGroupConcat extends TFixedTypeAggregator
{
    public static final TAggregator INSTANCE = new MGroupConcat();

    private MGroupConcat() {
        super("group_concat", MString.TEXT);
    }

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object del)
    {
        // skip all NULL rows
        if (source.isNull())
            return;

        // cache a StringBuilder instead?
        state.putString((state.hasAnyValue()
                            ? state.getString() + (String)del
                            : "") 
                            + source.getString(),
                        null);
    }

    @Override
    public void emptyValue(ValueTarget state)
    {
        state.putNull();
    }
}
