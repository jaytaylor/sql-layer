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

package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public abstract class SimplePValueIO implements PValueIO {

    protected abstract void copy(PValueSource in, TInstance typeInstance, PValueTarget out);

    @Override
    public void copyCanonical(PValueSource in, TInstance typeInstance, PValueTarget out) {
        copy(in, typeInstance, out);
    }

    @Override
    public void writeCollating(PValueSource in, TInstance typeInstance, PValueTarget out) {
        copy(in, typeInstance, out);
    }

    @Override
    public void readCollating(PValueSource in, TInstance typeInstance, PValueTarget out) {
        copy(in, typeInstance, out);
    }
}
