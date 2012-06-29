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

package com.akiban.server.t3expressions;

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.texpressions.TValidatedOverload;

import java.util.List;

public interface T3ScalarsRegistry {
    List<TValidatedOverload> getOverloads(String name);

    OverloadResolutionResult get(String name, List<? extends TClass> inputClasses);

    /**
     * Find the registered cast going from source to taret.
     * @param source Type to cast from
     * @param target Type to cast to
     * @return Return matching cast or <tt>null</tt> if none
     */
    TCast cast(TClass source, TClass target);

    /**
     * Returns the common of the two types. For either argument, a <tt>null</tt> value is interpreted as any type.
     * @param one the first type class
     * @param two the other type class
     * @return a wrapper that represents the common class, {@link #NO_COMMON} or {@link #ANY} (the latter only if both
     * inputs are <tt>null</tt>)
     */
    TClassPossibility commonTClass(TClass one, TClass two);


    /**
     * Represents the result that there is <i>no</i> common class.
     */
    public static final TClassPossibility NO_COMMON = new TClassPossibility() {
        @Override
        public boolean isAny() {
            return false;
        }

        @Override
        public boolean isNone() {
            return true;
        }

        @Override
        public TClass get() {
            return null;
        }
    };

    /**
     * Represents the result that <i>any</i> class is a common class.
     */
    public static final TClassPossibility ANY = new TClassPossibility() {
        @Override
        public boolean isAny() {
            return true;
        }

        @Override
        public boolean isNone() {
            return false;
        }

        @Override
        public TClass get() {
            return null;
        }
    };
}
