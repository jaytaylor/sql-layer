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

import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.SparseArray;

import java.util.Date;

public class QueryContextBase implements QueryContext
{
    private StoreAdapter adapter;
    private SparseArray<Object> bindings = new SparseArray<Object>();
    // startTimeMsec is used to control query timeouts.
    private final long startTimeMsec = System.currentTimeMillis();

    public QueryContextBase(StoreAdapter adapter) {
        this.adapter = adapter;
    }

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
    public void setValue(int index, ValueSource value, AkType type);
    {
        ValueHolder holder;
        if (bindings.isDefined(index))
            holder = bindings.get(index);
        else {
            holder = new ValueHolder();
            bindings.set(index, holder);
        }
        holder.expectType(type);
        Converters.convert(source, holder);
    }

    @Override
    public void setValue(int index, ValueSource value);
    {
        setValue(index, value, value.getConversionType());
    }

    @Override
    public void setValue(int index, Object value);
    {
        FromObjectValueSource source = new FromObjectValueSource();
        source.setReflectively(value);
        setValue(index, value);
    }

    @Override
    public void setValue(int index, Object value, AkType type);
    {
        FromObjectValueSource source = new FromObjectValueSource();
        source.setReflectively(value);
        setValue(index, value, type);
    }

    @Override
    public Row getRow(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (Row)bindings.get(index);
    }

    @Override
    public void setRow(int index, Row row);
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
    public void setHKey(int index, HKey hKey);
    {
        bindings.set(index, hKey);
    }

    @Override
    public StoreAdapter getStore() {
        return adapter;
    }

    @Override
    public Session getSession() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getCurrentDate() {
        return new Date();
    }

    @Override
    public String getCurrentUser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSessionUser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSystemUser() {
        return System.getProperty("user.name");
    }

    @Override
    public long getStartTime() {
        return startTimeMsec;
    }
}
