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

import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.texpressions.TValidatedOverload;

import java.util.List;
import java.util.UUID;

public interface T3ScalarsRegistery {
    OverladResolutionResult get(String name, List<? extends TClass> inputClasses);
    TCast cast(TClass source, TClass target);

    /**
     * Returns the common of the two types, or {@linkplain #NO_COMMON} if the two have no common type. For either
     * argument, a <tt>null</tt> value is interpreted as any type.
     * @param one the first type class
     * @param two the other type class
     * @return a wrapper that represents the common class, no common class, or <tt>ANY</tt> (the latter only if both
     * inputs are <tt>null</tt>)
     */
    TClassPossibility commonTClass(TClass one, TClass two);

    public static TBundleID NO_COMMON_BUNDLE
            = new TBundleID("<none>", UUID.fromString("AC13A852-BB2E-11E1-A692-133D6188709B"));

    public static TClass NO_COMMON = new TClass(NO_COMMON_BUNDLE, "<none>", Attribute.NONE, 1, 1, 0, null) {
        @Override
        public TFactory factory() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected TInstance doPickInstance(TInstance instance0, TInstance instance1) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void validate(TInstance instance) {
            throw new UnsupportedOperationException();
        }
    };
}
