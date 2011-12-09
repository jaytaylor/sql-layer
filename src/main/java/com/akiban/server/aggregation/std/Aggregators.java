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
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.service.functions.Aggregate;
import com.akiban.server.types.AkType;

public class Aggregators
{
    @Aggregate("bit_xor")
    public static AggregatorFactory bit_xor(final String name, final AkType type)
    {
        return new AggregatorFactory ()
        {
            @Override
            public Aggregator get() {return AbstractBitAggregator.getAgg(type, Processors.bitXOrProcessor); }

            @Override
            public String toString () {return name + type.name();}

            @Override
            public AkType outputType () { return type;}
        };
    }

    @Aggregate("bit_or")
    public static AggregatorFactory bit_or(final String name, final AkType type)
    {
        return new AggregatorFactory ()
        {
            @Override
            public Aggregator get() {return AbstractBitAggregator.getAgg(type, Processors.bitOrProcessor); }

            @Override
            public String toString () {return name + type.name();}

            @Override
            public AkType outputType () { return type;}
        };
    }

    @Aggregate("bit_and")
    public static AggregatorFactory bit_and(final String name, final AkType type)
    {
        return new AggregatorFactory ()
        {
            @Override
            public Aggregator get() {return AbstractBitAggregator.getAgg(type, Processors.bitAndProcessor); }

            @Override
            public String toString () {return name + type.name();}

            @Override
            public AkType outputType () { return type;}
        };
    }

    @Aggregate("min")
    public static AggregatorFactory mins (final String name, final AkType type)
    {
        return new AggregatorFactory ()
        {
            @Override
            public Aggregator get() {return AbstractAggregator.getAgg(type, Processors.minProcessor); }

            @Override
            public String toString () {return name + type.name();}
            
            @Override
            public AkType outputType () { return type;}
        };
    }

    @Aggregate("max")
    public static AggregatorFactory maxes (final String name, final AkType type)
    {
        return new AggregatorFactory ()
        {
            @Override
            public Aggregator get() {return AbstractAggregator.getAgg(type, Processors.maxProcessor); }

            @Override
            public String toString () {return name + type.name();}
           
            @Override
            public AkType outputType () { return type;}
        };
    }

    @Aggregate("sum")
    public static AggregatorFactory sum (final String name, final AkType type)
    {
        return new AggregatorFactory ()
        {
            @Override
            public Aggregator get() { return AbstractAggregator.getAgg(type, Processors.sumProcessor);}

            @Override
            public String toString() { return name + type.name();}
            
            @Override
            public AkType outputType () { return type;}
        };
    }

    private Aggregators() {}
}
