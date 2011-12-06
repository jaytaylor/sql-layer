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
import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.extract.BooleanExtractor;
import com.akiban.server.types.extract.DoubleExtractor;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.server.types.util.ValueHolder;
import java.math.BigDecimal;
import java.math.BigInteger;

class AbstractAggregator implements Aggregator
{
    protected static AbstractAggregator getAgg (AkType t, AbstractProcessor pr)
    {
        return new AbstractAggregator(pr,t);
    }

    protected  AkType type;
    protected AbstractProcessor processor;
    protected ValueHolder value;

    private static final DoubleExtractor D_EXTRACTOR = Extractors.getDoubleExtractor();
    private static final LongExtractor L_EXTRACTOR = Extractors.getLongExtractor(AkType.LONG);
    private static final LongExtractor DATE_EXTRACTOR = Extractors.getLongExtractor(AkType.DATE);
    private static final LongExtractor TIME_EXTRACTOR = Extractors.getLongExtractor(AkType.TIME);
    private static final LongExtractor DATETIME_EXTRACTOR = Extractors.getLongExtractor(AkType.DATETIME);
    private static final LongExtractor TIMESTAMP_EXTRACTOR = Extractors.getLongExtractor(AkType.TIMESTAMP);
    private static final ObjectExtractor<BigDecimal> DEC_EXTRACTOR = Extractors.getDecimalExtractor();
    private static final ObjectExtractor<BigInteger> BIGINT_EXTRACTOR = Extractors.getUBigIntExtractor();
    private static final BooleanExtractor B_EXTRACTOR = Extractors.getBooleanExtractor();
    private static final ObjectExtractor<String> S_EXTRACTOR = Extractors.getStringExtractor();
    
    /**
     *
     * @param processor
     * @param type
     */
    protected AbstractAggregator (AbstractProcessor processor, AkType type)
    {
        // check type
        processor.checkType(type);

        // initialise data fields
        this.type = type;
        this.processor = processor;
        value = null;
    }

    @Override
    public void input(ValueSource input)
    {
        if (!input.isNull())
        {
            if (value == null)
                value = new ValueHolder(input);
            else
                switch (type)
                {
                    case FLOAT:     
                    case U_FLOAT:   value.putFloat(processor.process((float)D_EXTRACTOR.getDouble(value), (float)D_EXTRACTOR.getDouble(input))); break;
                    case U_DOUBLE:
                    case DOUBLE:    value.putDouble(processor.process(D_EXTRACTOR.getDouble(value), D_EXTRACTOR.getDouble(input))); break;                      
                    case U_INT:     
                    case INT:
                    case LONG:      value.putLong(processor.process(L_EXTRACTOR.getLong(value), L_EXTRACTOR.getLong(input))); break;
                    case DECIMAL:   value.putDecimal(processor.process(DEC_EXTRACTOR.getObject(value), DEC_EXTRACTOR.getObject(input))); break;
                    case U_BIGINT:  value.putUBigInt(processor.process(BIGINT_EXTRACTOR.getObject(value), BIGINT_EXTRACTOR.getObject(input))); break;
                    case BOOL:      value.putBool(processor.process(B_EXTRACTOR.getBoolean(value, Boolean.TRUE), B_EXTRACTOR.getBoolean(input, Boolean.TRUE))); break;
                    case DATE:      value.putDate(processor.process(DATE_EXTRACTOR.getLong(value), DATE_EXTRACTOR.getLong(input))); break;
                    case TIME:      value.putTime(processor.process(TIME_EXTRACTOR.getLong(value), TIME_EXTRACTOR.getLong(input))); break;
                    case TEXT:
                    case VARCHAR:   value.putString(processor.process(S_EXTRACTOR.getObject(value), S_EXTRACTOR.getObject(input))); break;
                    case DATETIME:  value.putDateTime(processor.process(DATETIME_EXTRACTOR.getLong(value), DATETIME_EXTRACTOR.getLong(input))); break;
                    case TIMESTAMP: value.putTimestamp(processor.process(TIMESTAMP_EXTRACTOR.getLong(value), TIMESTAMP_EXTRACTOR.getLong(input))); break;
                    default: throw new InvalidArgumentTypeException( processor.toString() + " of " + type + " is not supported yet.");
                 }
        }
    }

    @Override
    public void output(ValueTarget output)
    {
        findOutPut(output);
    }

    protected void findOutPut (ValueTarget out)
    {
        if(value == null) value = new ValueHolder(emptyValue());

        Converters.convert(value, out);
        value = null;
    }

    @Override
    public ValueSource emptyValue()
    {
        return processor.emptyValue();
    }
}
