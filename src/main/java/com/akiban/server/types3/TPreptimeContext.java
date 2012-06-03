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

import com.akiban.util.SparseArray;

import java.util.List;

public final class TPreptimeContext {
    
    public TExecutionContext createExecutionContext() {
        return new TExecutionContext(preptimeCache, inputTypes, outputType);
    }
    
    public void set(int index, Object value) {
        if (preptimeCache == null)
            preptimeCache = new SparseArray<Object>(index);
        preptimeCache.set(index, value);
    }

    public TPreptimeContext(List<TInstance> inputTypes, TInstance outputType) {
        this.inputTypes = inputTypes;
        this.outputType = outputType;
    }

    private SparseArray<Object> preptimeCache;
    private List<TInstance> inputTypes;
    private TInstance outputType;
}
