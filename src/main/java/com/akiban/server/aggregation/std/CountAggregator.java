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

package com.akiban.server.aggregation.std;

import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;

public final class CountAggregator implements Aggregator {

    // Aggregator interface

    @Override
    public AkType outputType() {
        return AkType.LONG;
    }

    @Override
    public void input(ValueSource input) {
        if (countStar || (!input.isNull())) {
            ++ count;
        }
    }

    @Override
    public void output(ValueTarget output) {
        holder.putLong(count);
        count = 0;
        Converters.convert(holder, output);
    }

    // use in this package

    CountAggregator(boolean countStar) {
        this.countStar = countStar;
        this.holder = new ValueHolder();
    }

    // object state

    private final boolean countStar;
    private final ValueHolder holder;
    private long count;
}
