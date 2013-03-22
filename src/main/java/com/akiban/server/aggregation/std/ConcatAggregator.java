
package com.akiban.server.aggregation.std;

import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.service.functions.Aggregate;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.server.types.util.ValueHolder;

/**
 * GROUP_CONCAT([DISTINCT] expr [,expr ...]
             [ORDER BY {unsigned_integer | col_name | expr}
                 [ASC | DESC] [,col_name ...]]
             [SEPARATOR str_val])
 * 
 * 
 * 
 * Could be implemented as:
 * 
 *  - sort all the input columns
 *  - call GROUP_CONCAT (CONCAT(expr [,expr...]))
 * 
 */
public class ConcatAggregator implements Aggregator
{
    @Aggregate("group_concat")
    public static AggregatorFactory groupConcat(final String name, AkType type)
    {
        return new AggregatorFactory()
        {
            @Override
            public String toString()
            {
                return name;
            }

            @Override
            public Aggregator get(Object o)
            {
                return new ConcatAggregator((String) o);
            }

            @Override
            public Aggregator get()
            {
                return new ConcatAggregator(DEFAULT_DELIM);
            }

            @Override
            public AkType outputType()
            {
                return AkType.VARCHAR;
            }

            @Override
            public String getName()
            {
                return name;
            }
        };
    }
   
    private static final String DEFAULT_DELIM = ",";
    private final ValueHolder holder;
    private final String delim;
    private StringBuilder ret;
    private final ObjectExtractor<String> extractor = Extractors.getStringExtractor();
    
    private ConcatAggregator(String delim)
    {
        holder = new ValueHolder();
        ret = new StringBuilder();
        this.delim = delim;
    }

    @Override
    public void input(ValueSource input)
    {
        if (input.isNull())
            return;
        
        ret.append(extractor.getObject(input)).append(delim);
    }

    @Override
    public void output(ValueTarget output)
    {
        // delete the last delimeter
        if (ret.length() != 0)
            ret.delete(ret.length() - delim.length(), ret.length());
        holder.putString(ret.toString());
        ret = new StringBuilder();
        Converters.convert(holder, output);
    }

    @Override
    public ValueSource emptyValue()
    {
        return NullValueSource.only();
    }
}
