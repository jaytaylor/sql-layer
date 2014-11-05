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

import com.google.common.base.Predicate;

import java.util.List;

public interface TOverload {

    String id();

    /**
     *
     * Name that the user will see/use
     */
    String displayName();

    /**
     *
     * Name(s) used internally by the parser/registry.
     *
     * Most of the times, the two names are the same, but they could be different
     * for certain functions, especially those that need "special treatment"
     *
     * This needs to be an array because we could be defining different functions
     * with the same implementation
     */
    String[] registeredNames();

    TOverloadResult resultType();
    List<TInputSet> inputSets();
    InputSetFlags exactInputs();
    int[] getPriorities();
    Predicate<List<? extends TPreptimeValue>> isCandidate();
}
