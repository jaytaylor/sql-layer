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

package com.foundationdb.server.aggregation.std;

import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.extract.Extractors;
import com.foundationdb.server.types.util.ValueHolder;
import java.math.BigInteger;

class AbstractBitAggregator extends AbstractAggregator
{    
    protected static AbstractBitAggregator getAgg (AkType t, AbstractProcessor pr)
    {
        return new AbstractBitAggregator(pr,t);
    }

    private AbstractBitAggregator (AbstractProcessor processor, AkType type)
    {
        super(processor, type);
    }

    @Override
    public String toString ()
    {
        return processor.toString();
    }

    @Override
    public void input(ValueSource input)
    {
        if (!input.isNull())
        {
            BigInteger bigIntInput = Extractors.getUBigIntExtractor().getObject(input);
            if (value == null)
                value = new ValueHolder(AkType.U_BIGINT, bigIntInput);
            else
                value.putUBigInt(processor.process(Extractors.getUBigIntExtractor().getObject(value), bigIntInput));
        }
    }
}
