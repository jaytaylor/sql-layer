
package com.akiban.server.test.mt.mtatomics;

import com.akiban.server.test.mt.mtutil.TimePoints;

interface DelayerFactory {
    Delayer delayer(TimePoints timePoints);
}
