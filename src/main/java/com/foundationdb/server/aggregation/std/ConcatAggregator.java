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

package com.foundationdb.server.aggregation.std;

import com.foundationdb.server.aggregation.Aggregator;
import com.foundationdb.server.aggregation.AggregatorFactory;
import com.foundationdb.server.service.functions.Aggregate;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.ValueTarget;
import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.server.types.extract.Extractors;
import com.foundationdb.server.types.extract.ObjectExtractor;
import com.foundationdb.server.types.util.ValueHolder;

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
