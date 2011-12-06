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

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import java.math.BigInteger;

class AbstractBitAggregator extends AbstractAggregator
{
    protected static AbstractAggregator getAgg (AkType t, AbstractProcessor pr)
    {
        return new AbstractBitAggregator(pr,t);
    }

    private AbstractBitAggregator (AbstractProcessor processor, AkType type)
    {
        super(processor, type);
    }

    @Override
    public void input(ValueSource input)
    {
        BigInteger bigIntInput;
        if (input.isNull()) bigIntInput = BigInteger.ZERO;
        else bigIntInput = Extractors.getUBigIntExtractor().getObject(input);

        if (value == null)
            value = new ValueHolder(AkType.U_BIGINT, bigIntInput);
        else
            value.putUBigInt(processor.process(Extractors.getUBigIntExtractor().getObject(value), bigIntInput));
    }
}
