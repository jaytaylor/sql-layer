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
package com.foundationdb.server.types3.mcompat.mcasts;

import com.foundationdb.server.types3.TCast;
import com.foundationdb.server.types3.TCastBase;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;

public final class Cast_From_Float {
    public static final TCast TO_DOUBLE_UNSIGNED = new TCastBase(MApproximateNumber.FLOAT, MApproximateNumber.FLOAT_UNSIGNED) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            float orig = source.getFloat();
            if (orig < 0) {
                context.reportTruncate(Float.toString(orig), "0");
                orig = 0; // TODO or is it null?
            }
            target.putFloat(orig);
        }
    };

    private Cast_From_Float() {}
}
