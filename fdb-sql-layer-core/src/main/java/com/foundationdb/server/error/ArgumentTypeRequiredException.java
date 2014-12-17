/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.server.error;

import com.foundationdb.server.types.TInputSet;

public class ArgumentTypeRequiredException extends InvalidOperationException
{
    public ArgumentTypeRequiredException(String functionName, int argPosition) {
        this(functionName, Integer.toString(argPosition));
    }

    public ArgumentTypeRequiredException(String functionName, TInputSet inputSet) {
        this(functionName, coveringDesc(inputSet));
    }

    private ArgumentTypeRequiredException(String functionName, String inputDesc) {
        super(ErrorCode.ARGUMENT_TYPE_REQUIRED, functionName, inputDesc);
    }

    private static String coveringDesc(TInputSet inputSet) {
        StringBuilder sb = new StringBuilder();
        for(int i = inputSet.firstPosition(); i >= 0; i = inputSet.nextPosition(i+1)) {
            if(sb.length() > 0) {
                sb.append(",");
            }
            sb.append(i);
        }
        return sb.toString();
    }
}
