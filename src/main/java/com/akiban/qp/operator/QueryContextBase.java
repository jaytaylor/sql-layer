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

package com.akiban.qp.operator;

import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.service.session.Session;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.SparseArray;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public abstract class QueryContextBase implements QueryContext
{
    private SparseArray<Object> bindings = new SparseArray<Object>();
    // startTimeMsec is used to control query timeouts.
    private final long startTimeMsec = System.currentTimeMillis();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + bindings.describeElements() + "]";
    }

    /* QueryContext interface */

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
        catch (InvalidDateFormatException | InconvertibleTypesException | InvalidCharToNumException e)
        {
            errorCase(e, holder);
        }
    }
    
    private void errorCase (InvalidOperationException e, ValueHolder holder)
    {
        warnClient(e);
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
    public void setValue(int index, Object value)
    {
        FromObjectValueSource source = new FromObjectValueSource();
        source.setReflectively(value);
        setValue(index, source);
    }

    @Override
    public void setValue(int index, Object value, AkType type)
    {
        FromObjectValueSource source = new FromObjectValueSource();
        source.setReflectively(value);
        setValue(index, source, type);
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
    public Date getCurrentDate() {
        return new Date();
    }

    @Override
    public String getSystemUser() {
        return System.getProperty("user.name");
    }

    @Override
    public long getStartTime() {
        return startTimeMsec;
    }

    @Override
    public void warnClient(InvalidOperationException exception) {
        notifyClient(NotificationLevel.WARNING, exception.getCode(), exception.getShortMessage());
    }

}
