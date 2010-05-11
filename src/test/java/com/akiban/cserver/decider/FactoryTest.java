/**
 * 
 */
package com.akiban.cserver.decider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.akiban.cserver.CServerConfig;
/**
 * @author percent
 *
 */

public class FactoryTest {

	/**
	 * Test method for {@link com.akiban.cserver.decider.Factory#getSingleton()}.
	 */
	@Test
	public void testGetSingleton() {
		Factory f = Factory.getSingleton();
		assertEquals(Factory.class.hashCode(), f.getClass().hashCode());
		
		Factory f1 = Factory.getSingleton();
		assertEquals(f, f1);
		Factory f2 = Factory.getSingleton();
		assertEquals(f1, f2);
	}

	/**
	 * Test method for {@link com.akiban.cserver.decider.Factory#getType()}.
	 */
	@Test
	public void testGetType() {
		Factory f = Factory.getSingleton();
		assertEquals(Factory.DeciderType.RR, f.getType());
		CServerConfig config = new CServerConfig();
		
		config.getProperties().put("cserver.decision_engine", "hstore");
		Factory.setConfig(config);
		f = Factory.getSingleton();
		assertEquals(Factory.DeciderType.HStore, f.getType());
		
		config.getProperties().put("cserver.decision_engine", "vstore");
		Factory.setConfig(config);
		f = Factory.getSingleton();
		assertEquals(Factory.DeciderType.VStore, f.getType());
		
		config.getProperties().put("cserver.decision_engine", "fuck-show");
		boolean error = false;
		try {
			Factory.setConfig(config);
		} catch(AssertionError e) {
			error = true;
		}
		assertTrue(error);
		f = Factory.getSingleton();
		assertEquals(Factory.DeciderType.RR, f.getType());
	}

	/**
	 * Test method for {@link com.akiban.cserver.decider.Factory#createDecisionEngine()}.
	 */
	@Test
	public void testCreateDecisionEngine() {
		Factory f = Factory.getSingleton();
		Decider d = f.createDecisionEngine();
		assertTrue(d instanceof RoundRobin);
		CServerConfig config = new CServerConfig();
		
		config.getProperties().put("cserver.decision_engine", "hstore");
		Factory.setConfig(config);
		f = Factory.getSingleton();
		d = f.createDecisionEngine();
		assertTrue(d instanceof Monadic);
		assertEquals(Decider.EngineType.HStore, d.decide(null));

		config.getProperties().put("cserver.decision_engine", "vstore");
		Factory.setConfig(config);
		f = Factory.getSingleton();
		d = f.createDecisionEngine();
		assertTrue(d instanceof Monadic);
		assertEquals(Decider.EngineType.VStore, d.decide(null));
	}

}
