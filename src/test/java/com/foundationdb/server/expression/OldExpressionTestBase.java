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

package com.foundationdb.server.expression;

import com.foundationdb.server.t3expressions.T3RegistryServiceImpl;
import com.foundationdb.server.t3expressions.TCastResolver;
import com.foundationdb.server.types3.Types3Switch;
import org.junit.After;
import org.junit.Before;

public abstract class OldExpressionTestBase {
    private boolean types3switch;

    @Before
    public final void setTypes3Switch() {
        types3switch = Types3Switch.ON;
        Types3Switch.ON = false;
    }

    @After
    public final void restoreTypes3Switch() {
        Types3Switch.ON = types3switch;
    }

    protected static synchronized TCastResolver castResolver() {
        if (castResolver == null)
            castResolver = T3RegistryServiceImpl.createTCastResolver();
        return castResolver;
    }

    private static TCastResolver castResolver;
}
