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
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.ArgumentValidation;

final class LongAggregator implements Aggregator {
    @Override
    public AkType outputType() {
        return type;
    }

    @Override
    public void input(ValueSource input) {
        value =  processor.process(value, extractor.getLong(input));
        sawAny = true;
    }

    @Override
    public void output(ValueTarget output) {
        assert sawAny : "didn't see any input rows!";
        Converters.convert(new ValueHolder(type, value), output);
    }

    protected LongAggregator(LongProcessor processor, AkType type) {
        this.type = type;
        if (type.underlyingType() != AkType.LONG.underlyingType()) {
            throw new IllegalArgumentException("type must have an underlying long: " + type);
        }
        this.extractor = Extractors.getLongExtractor(this.type);
        this.processor = processor;
        ArgumentValidation.notNull("processor", this.processor);
    }

    private final AkType type;
    private final LongExtractor extractor;
    private final LongProcessor processor;
    private long value;
    private boolean sawAny = false;

    // subclasses
    public interface LongProcessor {
        long process(long oldState, long input);
    }
}
