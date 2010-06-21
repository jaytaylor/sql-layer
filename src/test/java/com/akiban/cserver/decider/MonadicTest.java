/**
 * 
 */
package com.akiban.cserver.decider;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author percent
 * 
 */
public class MonadicTest {

    /**
     * Test method for
     * {@link com.akiban.cserver.decider.Monadic#decide(com.akiban.message.Request)}
     * .
     */
    @Test
    public void testDecide() {
        Monadic m = new Monadic(Decider.RowCollectorType.VCollector);
        assertEquals(Decider.RowCollectorType.VCollector, m.decide(null, null));
        assertEquals(Decider.RowCollectorType.VCollector, m.decide(null, null));
        assertEquals(Decider.RowCollectorType.VCollector, m.decide(null, null));

        m = new Monadic(Decider.RowCollectorType.PersistitRowCollector);
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, m.decide(
                null, null));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, m.decide(
                null, null));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, m.decide(
                null, null));
    }

}
