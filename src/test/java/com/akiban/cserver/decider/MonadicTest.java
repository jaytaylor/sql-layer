/**
 * 
 */
package com.akiban.cserver.decider;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author percent
 *
 */
public class MonadicTest {

	/**
	 * Test method for {@link com.akiban.cserver.decider.Monadic#decide(com.akiban.message.Request)}.
	 */
	@Test
	public void testDecide() {
		Monadic m = new Monadic(Decider.EngineType.VStore);
		assertEquals(Decider.EngineType.VStore, m.decide(null));
		assertEquals(Decider.EngineType.VStore, m.decide(null));
		assertEquals(Decider.EngineType.VStore, m.decide(null));
		
		m = new Monadic(Decider.EngineType.HStore);
		assertEquals(Decider.EngineType.HStore, m.decide(null));
		assertEquals(Decider.EngineType.HStore, m.decide(null));
		assertEquals(Decider.EngineType.HStore, m.decide(null));
	}

}
