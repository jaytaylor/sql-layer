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

package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;

abstract class LongConverter extends AbstractConverter {

    // defined in subclasses

    protected abstract void putLong(ValueTarget target, long value);

    @Override
    protected AkType targetConversionType() {
        return extractor().targetConversionType();
    }

    @Override
    protected final void doConvert(ValueSource source, ValueTarget target) {
        putLong(target, extractor().getLong(source));
    }

    protected final LongExtractor extractor() {
        return extractor;
    }

    protected LongConverter(AkType type) {
        extractor = Extractors.getLongExtractor(type);
    }

    private final LongExtractor extractor;

    // consts


    static final LongConverter LONG = new LongConverter(AkType.LONG) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putLong(value);
        }
    };

    static final LongConverter INT = new LongConverter(AkType.INT) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putInt(value);
        }
    };

    static final LongConverter U_INT = new LongConverter(AkType.U_INT) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putUInt(value);
        }
    };

    static final LongConverter DATE = new LongConverter(AkType.DATE) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putDate(value);
        }
    };

    static final LongConverter DATETIME = new LongConverter(AkType.DATETIME) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putDateTime(value);
        }
    };

    static final LongConverter TIME = new LongConverter(AkType.TIME) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putTime(value);
        }
    };

    static final LongConverter TIMESTAMP = new LongConverter(AkType.TIMESTAMP) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putTimestamp(value);
        }
    };

    static final LongConverter INTERVAL_MILLIS = new LongConverter(AkType.INTERVAL_MILLIS)
    {
        @Override
        protected void putLong(ValueTarget target, long value)
        {
            target.putInterval_Millis(value);
        }
    };

    static final LongConverter INTERVAL_MONTH = new LongConverter(AkType.INTERVAL_MONTH)
    {
        @Override
        protected void putLong(ValueTarget target, long value)
        {
            target.putInterval_Month(value);
        }
    };

    static final LongConverter YEAR = new LongConverter(AkType.YEAR) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putYear(value);
        }
    };
}
