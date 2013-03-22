
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
            public String getName () {return name;}

            @Override
            public String toString () {return name + type.name();}

            @Override
            public AkType outputType () { return type;}
            
            @Override
            public Aggregator get(Object obj)
            {
                return get();
            }
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
            public String getName () {return name;}

            @Override
            public String toString () {return name + type.name();}

            @Override
            public AkType outputType () { return type;}
            
            @Override
            public Aggregator get(Object obj)
            {
                return get();
            }
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
            public String getName () {return name;}

            @Override
            public String toString () {return name + type.name();}

            @Override
            public AkType outputType () { return type;}

            @Override
            public Aggregator get(Object obj)
            {
                return get();
            }
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
            public String getName () {return name;}

            @Override
            public String toString () {return name + type.name();}
            
            @Override
            public AkType outputType () { return type;}
             
            @Override
            public Aggregator get(Object obj)
            {
                return get();
            }
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
            public String getName () {return name;}

            @Override
            public String toString () {return name + type.name();}
           
            @Override
            public AkType outputType () { return type;}
            
            @Override
            public Aggregator get(Object obj)
            {
                return get();
            }
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
            public String getName () {return name;}

            @Override
            public String toString() { return name + type.name();}
            
            @Override
            public AkType outputType () { return type;}
            
            @Override
            public Aggregator get(Object obj)
            {
                return get();
            }
        };
    }

    private Aggregators() {}
}
