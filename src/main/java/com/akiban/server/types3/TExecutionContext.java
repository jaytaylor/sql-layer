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

package com.akiban.server.types3;

import com.akiban.qp.operator.QueryContext;
import com.akiban.util.SparseArray;

import java.util.List;

public final class TExecutionContext {

    public TInstance inputTInstanceAt(int index) {
        return inputTypes.get(index);
    }
    
    public TInstance outputTInstance() {
        return outputType;
    }
    
    public Object preptimeObjectAt(int index) {
        if (preptimeCache == null)
            throw new IllegalArgumentException("no preptime cache objects");
        return preptimeCache.getIfDefined(index);
    }
    
    public Object hasExectimeObject(int index) {
        return exectimeCache != null && exectimeCache.isDefined(index);
    }
    
    public Object exectimeObjectAt(int index) {
        if (exectimeCache == null)
            throw new IllegalArgumentException("no exectime cache objects");
        return exectimeCache.getIfDefined(index);
    }
    
    public void putExectimeObject(int index, Object value) {
        if (exectimeCache == null)
            exectimeCache = new SparseArray<Object>(index);
        exectimeCache.set(index, value);
    }

    TExecutionContext(SparseArray<Object> preptimeCache, List<TInstance> inputTypes, TInstance outputType) {
        this.preptimeCache = preptimeCache;
        this.inputTypes = inputTypes;
        this.outputType = outputType;
    }

    private SparseArray<Object> preptimeCache;
    private SparseArray<Object> exectimeCache;
    private List<TInstance> inputTypes;
    private TInstance outputType;
}
