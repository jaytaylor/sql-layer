/**
 * 
 */
package com.akiban.cserver.decider;

import static org.junit.Assert.*;

import org.junit.Test;

import com.akiban.cserver.decider.Decider;
import com.akiban.cserver.decider.RoundRobin;

/**
 * @author percent
 *
 */
public class RoundRobinTest {

	/**
	 * Test method for {@link com.akiban.cserver.decider.RoundRobin#decide(com.akiban.message.Request)}.
	 */
	@Test
	public void testDecide() {
		RoundRobin rr = new RoundRobin();
		assertEquals(rr.decide(null), Decider.EngineType.VStore);
		assertEquals(rr.decide(null), Decider.EngineType.HStore);
		assertEquals(rr.decide(null), Decider.EngineType.VStore);
		assertEquals(rr.decide(null), Decider.EngineType.HStore);
	}

}
