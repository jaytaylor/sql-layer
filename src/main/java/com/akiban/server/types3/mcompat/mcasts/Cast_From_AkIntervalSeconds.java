/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.aksql.aktypes.AkInterval;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class Cast_From_AkIntervalSeconds {
    
    public static TCast TO_DOUBLE = new TCastBase(AkInterval.SECONDS, MApproximateNumber.DOUBLE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            long raw = source.getInt64();
            double result = 0;

            boolean isNegative;
            if (raw < 0) {
                isNegative = true;
                raw = -raw;
            }
            else {
                isNegative = false;
            }

            // we rely here on the fact that the EnumMap sorts entries by ordinal, _and_ that the ordinals
            // in TimeUnit are organized  by small unit -> big.
            for (Map.Entry<TimeUnit, Double> unitAndMultiplier : placeMultiplierMap.entrySet()) {
                TimeUnit unit = unitAndMultiplier.getKey();

                long asLong = AkInterval.secondsIntervalAs(raw, unit);
                assert (asLong >= 0) && (asLong < 100) : asLong;
                raw -= AkInterval.secondsRawFrom(asLong, unit);

                double multiplier = unitAndMultiplier.getValue();
                result += (multiplier * asLong);
            }

            if (isNegative)
                result *= -1d;

            target.putDouble(result);
        }
    };

    private static EnumMap<TimeUnit, Double> placeMultiplierMap = createPlaceMultiplierMap();

    private static EnumMap<TimeUnit, Double> createPlaceMultiplierMap() {
        // Desired result is something like 20120802120133.000000
        // Nicer format: yyyyMMddHHmmSS.uuuuuu 
        EnumMap<TimeUnit, Double> result = new EnumMap<TimeUnit, Double>(TimeUnit.class);
        result.put(TimeUnit.MICROSECONDS, 1.0d/1000000.0d);
        result.put(TimeUnit.SECONDS,      1d);
        result.put(TimeUnit.MINUTES,    100d);
        result.put(TimeUnit.HOURS,    10000d);
        result.put(TimeUnit.DAYS,   1000000d);
        return result;
    }
}
