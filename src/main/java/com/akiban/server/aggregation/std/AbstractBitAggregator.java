
package com.akiban.server.aggregation.std;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
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
