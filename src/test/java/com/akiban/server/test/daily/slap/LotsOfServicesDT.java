
package com.akiban.server.test.daily.slap;

import com.akiban.server.test.daily.DailyBase;
import com.akiban.server.test.mt.mtutil.Timing;
import org.junit.Test;

public final class LotsOfServicesDT extends DailyBase {
    @Test
    public void loop() throws Exception{

        stopTestServices(); // shut down ApiTestBase's @Before services

        final int LOOP_COUNT = 1000;
        int i=0;
        try {
            for (; i < LOOP_COUNT; ++i) {
                startTestServices();
                Timing.sleep(10);
                stopTestServices();
            }
        } catch (Throwable e) {
            throw new RuntimeException("At i="+i, e);
        }

        startTestServices(); // so that ApiTestBase's @After has something to shut down
    }
}
