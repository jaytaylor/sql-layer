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

package com.akiban.server.types3.aksql.aktypes;

import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TClassFormatter;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.aksql.AkBundle;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.AkibanAppender;

import java.util.Formatter;
import java.util.concurrent.TimeUnit;

public final class AkIntervalSeconds extends IntervalBase {

    /**
     * An SECONDS interval. Its underlying INT_64 represents microseconds, but you should not rely on that, because
     * it may change (for instance, to nanoseconds).  Instead, use #rawValueAs to get a PValueSource's values in
     * whatever unit you want.
     */
    public static IntervalBase SECONDS = new AkIntervalSeconds();

    public static long rawValueAs(PValueSource source, TimeUnit as) {
        return rawValueAs(source.getInt64(), as);
    }

    public static long rawValueAs(long secondsIntervalRaw, TimeUnit as) {
        return as.convert(secondsIntervalRaw, AkIntervalSecondsFormat.UNDERLYING_UNIT);
    }

    private enum SecondsAttrs implements Attribute {
        FORMAT
    }

    private AkIntervalSeconds() {
        super(
                AkBundle.INSTANCE.id(),
                "interval seconds",
                AkCategory.DATE_TIME,
                SecondsAttrs.class,
                formatter,
                1,
                1,
                8,
                PUnderlying.INT_64,
                SecondsAttrs.FORMAT,
                AkIntervalSecondsFormat.values());
    }

    private static TClassFormatter formatter = new TClassFormatter() {
        @Override
        public void format(TInstance instance, PValueSource source, AkibanAppender out) {
            long micros = rawValueAs(source, TimeUnit.MICROSECONDS);

            long days = rawValueAs(micros, TimeUnit.DAYS);
            micros -= TimeUnit.DAYS.toMicros(days);

            long hours = rawValueAs(micros, TimeUnit.HOURS);
            micros -= TimeUnit.HOURS.toMicros(hours);

            long minutes = rawValueAs(micros, TimeUnit.MINUTES);
            micros -= TimeUnit.MINUTES.toMicros(minutes);

            long seconds = rawValueAs(micros, TimeUnit.SECONDS);
            micros -= TimeUnit.SECONDS.toMicros(seconds);

            Formatter formatter = new Formatter(out.getAppendable());
            formatter.format("INTERVAL '%d %d:%d:%d.%05d", days, hours, minutes, seconds, micros);
        }
    };
}
