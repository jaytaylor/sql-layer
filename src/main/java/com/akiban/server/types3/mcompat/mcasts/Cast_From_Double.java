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

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TCastPath;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public final class Cast_From_Double {

    public static final TCast FLOAT_TO_DOUBLE = new TCastBase(MApproximateNumber.FLOAT, MApproximateNumber.DOUBLE) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putDouble(source.getFloat());
        }
    };

    public static final TCast FLOAT_UNSIGNED_TO_DOUBLE = new TCastBase(MApproximateNumber.FLOAT_UNSIGNED, MApproximateNumber.DOUBLE) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putDouble(source.getFloat());
        }
    };

    public static final TCast DOUBLE_UNSIGNED_TO_DOUBLE = new TCastBase(MApproximateNumber.DOUBLE_UNSIGNED, MApproximateNumber.DOUBLE) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putDouble(source.getDouble());
        }
    };

    public static final TCastPath FLOAT_TO_VARCHAR = TCastPath.create(
            MApproximateNumber.FLOAT,
            MApproximateNumber.DOUBLE,
            MString.VARCHAR
    );

    public static final TCastPath FLOAT_UNSIGNED_TO_VARCHAR = TCastPath.create(
            MApproximateNumber.FLOAT_UNSIGNED,
            MApproximateNumber.DOUBLE,
            MString.VARCHAR
    );

    public static final TCastPath DOUBLE_UNSIGNED_TO_VARCHAR = TCastPath.create(
            MApproximateNumber.DOUBLE_UNSIGNED,
            MApproximateNumber.DOUBLE,
            MString.VARCHAR
    );

    public static final TCast TO_FLOAT = new TCastBase(MApproximateNumber.DOUBLE, MApproximateNumber.FLOAT) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            double orig = source.getDouble();
            double positive = Math.abs(orig);
            if (positive > Float.MAX_VALUE) {
                context.reportTruncate(Double.toString(orig), Float.toString(Float.MAX_VALUE));
                orig = Float.MAX_VALUE; // TODO or is it null?
            }
            target.putFloat((float)orig);
        }
    };

    public static final TCast TO_FLOAT_UNSIGNED = new TCastBase(MApproximateNumber.DOUBLE, MApproximateNumber.FLOAT_UNSIGNED) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            double orig = source.getDouble();
            if (orig < 0) {
                context.reportTruncate(Double.toString(orig), "0");
                orig = 0; // TODO or is it null?
            }
            else if (orig > Float.MAX_VALUE) {
                context.reportTruncate(Double.toString(orig), Float.toString(Float.MAX_VALUE));
                orig = Float.MAX_VALUE; // TODO or is it null?
            }
            target.putFloat((float)orig);
        }
    };

    public static final TCast TO_DOUBLE_UNSIGNED = new TCastBase(MApproximateNumber.DOUBLE, MApproximateNumber.DOUBLE_UNSIGNED) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            double orig = source.getDouble();
            if (orig < 0) {
                context.reportTruncate(Double.toString(orig), "0");
                orig = 0; // TODO or is it null?
            }
            target.putDouble(orig);
        }
    };
}
