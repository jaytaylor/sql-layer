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

package com.akiban.qp.operator;

import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.error.*;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.util.BloomFilter;
import com.akiban.util.SparseArray;

import java.math.BigDecimal;
import java.math.BigInteger;

public class SparseArrayQueryBindings implements QueryBindings
{
    private SparseArray<Object> bindings = new SparseArray<>();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + bindings.describeElements() + "]";
    }

    /* QueryBindings interface */

    @Override
    public PValueSource getPValue(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (PValueSource)bindings.get(index);
    }

    /*
     * (non-Javadoc)
     * @see com.akiban.qp.operator.QueryContext#setPValue(int, com.akiban.server.types3.pvalue.PValueSource)
     * This makes a copy of the PValueSource value, rather than simply
     * storing the reference. The assumption is the PValueSource parameter
     * will be reused by the caller as rows are processed, so the QueryContext
     * needs to keep a copy of the underlying value.
     *
     */
    @Override
    public void setPValue(int index, PValueSource value) {
        PValue holder = null;
        if (bindings.isDefined(index)) {
            holder = (PValue)bindings.get(index);
            if (holder.tInstance() != value.tInstance())
                holder = null;
        }
        if (holder == null) {
            holder = new PValue(value.tInstance());
            bindings.set(index, holder);
        }
        PValueTargets.copyFrom(value, holder);
    }
    
    @Override
    public ValueSource getValue(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (ValueSource)bindings.get(index);
    }

    @Override
    public void setValue(int index, ValueSource value, AkType type)
    {
        ValueHolder holder;
        if (bindings.isDefined(index))
            holder = (ValueHolder)bindings.get(index);
        else {
            holder = new ValueHolder();
            bindings.set(index, holder);
        }
        
        holder.expectType(type);
        try
        {
            Converters.convert(value, holder);
        }
        catch (InvalidDateFormatException e)
        {
            errorCase(e, holder);
        }
        catch (InconvertibleTypesException e)
        {
            errorCase(e, holder);
        }
        catch (InvalidCharToNumException e)
        {
            errorCase(e, holder);
        }
    }
    
    private void errorCase (InvalidOperationException e, ValueHolder holder)
    {
        //warnClient(e);
        switch(holder.getConversionType())
        {
            case DECIMAL:   holder.putDecimal(BigDecimal.ZERO); break;
            case U_BIGINT:  holder.putUBigInt(BigInteger.ZERO); break;
            case LONG:
            case U_INT:
            case INT:        holder.putRaw(holder.getConversionType(), 0L); break;
            case U_DOUBLE:   
            case DOUBLE:     holder.putRaw(holder.getConversionType(), 0.0d);
            case U_FLOAT:
            case FLOAT:      holder.putRaw(holder.getConversionType(), 0.0f); break;
            case TIME:       holder.putTime(0L);
            default:         holder.putNull();

        }
    }

    @Override
    public void setValue(int index, ValueSource value)
    {
        setValue(index, value, value.getConversionType());
    }

    @Override
    public Row getRow(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (Row)bindings.get(index);
    }

    @Override
    public void setRow(int index, Row row)
    {
        // TODO: Should this use a RowHolder or will that make things worse?
        bindings.set(index, row);
    }

    @Override
    public HKey getHKey(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (HKey)bindings.get(index);
    }

    @Override
    public void setHKey(int index, HKey hKey)
    {
        bindings.set(index, hKey);
    }

    @Override
    public BloomFilter getBloomFilter(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (BloomFilter)bindings.get(index);
    }

    @Override
    public void setBloomFilter(int index, BloomFilter filter) {
        bindings.set(index, filter);
    }

    @Override
    public void clear() {
        bindings.clear();
    }
}
