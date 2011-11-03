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
import java.math.BigDecimal;
import java.math.BigInteger;

class Processors
{
    public final static AbstractProcessor maxProcessor = new MinMaxProcessor()
    {
        @Override
        public boolean condition (double a) { return a > 0; }

    };

    public final static AbstractProcessor minProcessor = new MinMaxProcessor()
    {
        @Override
        public boolean condition(double a) { return a < 0; }

    };

    public final static AbstractProcessor sumProcessor = new AbstractProcessor ()
    {
        @Override
        public void checkType (AkType type)
        {
            switch (type)
            {
                case DOUBLE:
                case INT:
                case LONG:
                case DECIMAL:
                case U_BIGINT: return;
                default: throw new UnsupportedOperationException("Not supported yet.");
            }
        }

        @Override
        public long process(long oldState, long input)
        {
            return oldState + input;
        }

        @Override
        public double process(double oldState, double input)
        {
            return oldState + input;
        }

        @Override
        public BigDecimal process(BigDecimal oldState, BigDecimal input)
        {
            return oldState.add(input);
        }

        @Override
        public BigInteger process(BigInteger oldState, BigInteger input)
        {
            return oldState.add(input);
        }

        @Override
        public boolean process(boolean oldState, boolean input)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String process(String oldState, String input)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    };

    // nested class
    private static abstract class MinMaxProcessor implements AbstractProcessor
    {
        abstract boolean condition (double a);

        @Override
        public void checkType(AkType type)
        {
            switch (type)
            {
                case DOUBLE:
                case LONG:
                case DECIMAL:
                case U_BIGINT:
                case VARCHAR:
                case DATE:
                case BOOL:
                case DATETIME:
                case TIME:      return;
                default:        throw new UnsupportedOperationException("Not supported yet.");
            }            
        }

        @Override
        public long process(long oldState, long input)
        {
            return (condition(oldState - input) ? oldState : input);
        }

        @Override
        public double process(double oldState, double input)
        {
            return (condition(oldState - input) ? oldState : input);
        }

        @Override
        public BigDecimal process(BigDecimal oldState, BigDecimal input)
        {
            return (condition(oldState.compareTo(input))? oldState : input);
        }

        @Override
        public BigInteger process(BigInteger oldState, BigInteger input)
        {
            return (condition(oldState.compareTo(input)) ? oldState : input);
        }

        @Override
        public boolean process(boolean oldState, boolean input)
        {
            return condition(1);
        }

        @Override
        public String process(String oldState, String input)
        {
            return (condition(oldState.compareTo(input)) ? oldState : input);
        }
    }
}
