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

package com.akiban.server.types3.mcompat.aggr;

import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TFixedTypeAggregator;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public class MGroupConcat extends TFixedTypeAggregator
{
    public static final TAggregator INSTANCE = new MGroupConcat();

    private MGroupConcat() {
        super("group_concat", MString.TEXT);
    }

    @Override
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state, Object del)
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
    public void emptyValue(PValueTarget state)
    {
        state.putNull();
    }
}
