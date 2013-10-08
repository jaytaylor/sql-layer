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

package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.common.funcs.TTrigs;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;

public class MTrigs
{
    // This is a 'strange' case
    //
    // in aksql, the return type would always be AkNumeric.DOUBLE.instance()
    //
    // but in mysql, there could be multiple instances of the same TClass
    // (each differing from each other by the width)
    // So we'd define a fixed/default width that this function returns
    
    public static final TScalar TRIGS[] = TTrigs.create(MApproximateNumber.DOUBLE);
}
