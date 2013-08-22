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

package com.foundationdb.server.explain;

import java.util.HashMap;
import java.util.Map;

public class ExplainContext
{
    private final Map<Explainable,CompoundExplainer> extraInfo = new HashMap<>();

    public ExplainContext() {
    }

    /** Extra info is like debug info in the output from a compiler:
     * information that the optimizer had that helps explain the plan
     * but isn't needed for proper execution. */
    public CompoundExplainer getExtraInfo(Explainable explainable) {
        return extraInfo.get(explainable);
    }

    public boolean hasExtraInfo(Explainable explainable) {
        return extraInfo.containsKey(explainable);
    }

    public void putExtraInfo(Explainable explainable, CompoundExplainer info) {
        CompoundExplainer old = extraInfo.put(explainable, info);
        assert (old == null);
    }
}
