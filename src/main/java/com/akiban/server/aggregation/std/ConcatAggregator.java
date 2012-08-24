/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
