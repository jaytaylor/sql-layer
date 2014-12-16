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

package com.foundationdb.server.types;

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.util.SparseArray;
import org.joda.time.DateTimeZone;

import java.util.List;

public final class TPreptimeContext {
    public List<TInstance> getInputTypes() {
        return inputTypes;
    }

    public TInstance inputTypeAt(int index) {
        return inputTypes.get(index);
    }

    public TInstance getOutputType() {
        return outputType;
    }

    public void setOutputType(TInstance outputType) {
        this.outputType = outputType;
    }

    public TExecutionContext createExecutionContext() {
        return new TExecutionContext(preptimeCache, inputTypes, outputType, 
                queryContext,
                null, null, null); // TODO pass in
    }
    
    public String getCurrentTimezone()
    {
        // TODO need to get this from the session
        return DateTimeZone.getDefault().getID();
    }
    
    public String getLocale()
    {
        // TODO:
        throw new UnsupportedOperationException("not supported yet");
    }
    
    public Object get(int index) {
        if ((preptimeCache != null) && preptimeCache.isDefined(index))
            return preptimeCache.get(index);
        else
            return null;
    }

    public void set(int index, Object value) {
        if (preptimeCache == null)
            preptimeCache = new SparseArray<>(index);
        preptimeCache.set(index, value);
    }

    public SparseArray<Object> getValues() {
        return preptimeCache;
    }

    public TPreptimeContext(List<TInstance> inputTypes, QueryContext queryContext) {
        this.inputTypes = inputTypes;
        this.queryContext = queryContext;
    }

    public TPreptimeContext(List<TInstance> inputTypes, TInstance outputType, QueryContext queryContext) {
        this.inputTypes = inputTypes;
        this.outputType = outputType;
        this.queryContext = queryContext;
    }

    public TPreptimeContext(List<TInstance> inputTypes, TInstance outputType, QueryContext queryContext, SparseArray<Object> preptimeCache) {
        this.inputTypes = inputTypes;
        this.outputType = outputType;
        this.queryContext = queryContext;
        this.preptimeCache = preptimeCache;
    }

    private final QueryContext queryContext;
    private SparseArray<Object> preptimeCache;
    private List<TInstance> inputTypes;
    private TInstance outputType;
}
