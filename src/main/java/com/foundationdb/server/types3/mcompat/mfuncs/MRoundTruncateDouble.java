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
package com.foundationdb.server.types3.mcompat.mfuncs;

import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.TCustomOverloadResult;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TScalar;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.TPreptimeContext;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.common.types.DoubleAttribute;
import com.foundationdb.server.types3.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MRoundTruncateDouble extends TScalarBase {

    public static final Collection<TScalar> overloads = createAll();

    private enum RoundingStrategy {
        ROUND(RoundingMode.HALF_UP),
        TRUNCATE(RoundingMode.DOWN)
        ;

        public double apply(double input, int scale) {
            BigDecimal asDecimal = BigDecimal.valueOf(input);
            asDecimal = asDecimal.setScale(scale, roundingMode);
            return asDecimal.doubleValue();
        }

        private RoundingStrategy(RoundingMode roundingMode) {
            this.roundingMode = roundingMode;
        }

        private final RoundingMode roundingMode;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        double input = inputs.get(0).getDouble();
        int scale = signatureStrategy.roundToScale(inputs);
        double result = roundingStrategy.apply(input, scale);
        output.putDouble(result);
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                PValueSource incomingScale = signatureStrategy.getScaleOperand(inputs);
                int resultScale = (incomingScale == null)
                        ? inputs.get(0).instance().attribute(DoubleAttribute.SCALE)
                        : incomingScale.getInt32();
                int resultPrecision = 17 + resultScale;
                return MApproximateNumber.DOUBLE.instance(resultPrecision, resultScale, anyContaminatingNulls(inputs));
            }
        });
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        signatureStrategy.buildInputSets(MApproximateNumber.DOUBLE, builder);
    }

    @Override
    public String displayName() {
        return roundingStrategy.name();
    }

    protected MRoundTruncateDouble(RoundingOverloadSignature signatureStrategy,
                                   RoundingStrategy roundingStrategy)
    {
        this.signatureStrategy = signatureStrategy;
        this.roundingStrategy = roundingStrategy;
    }

    private static Collection<TScalar> createAll() {
        List<TScalar> results = new ArrayList<>();
        for (RoundingOverloadSignature signature : RoundingOverloadSignature.values()) {
            for (RoundingStrategy rounding : RoundingStrategy.values()) {
                results.add(new MRoundTruncateDouble(signature, rounding));
            }
        }
        return Collections.unmodifiableCollection(results);
    }

    private final RoundingOverloadSignature signatureStrategy;
    private final RoundingStrategy roundingStrategy;
}
