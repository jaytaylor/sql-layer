/**
 * 
 */
package com.akiban.cserver.decider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.message.ScanRowsRequest;
/**
 * @author percent
 *
 */

public class DecisionEngineTest {

	@Test
	public void testGetStrategy() {
        CServerConfig config = new CServerConfig();
        DecisionEngine de = DecisionEngine.createDecisionEngine(config);
		assertEquals(DecisionEngine.Strategy.HStore, de.getStrategy());
	    
        config.getProperties().put("cserver.decision_engine", "vstore");
	    de = DecisionEngine.createDecisionEngine(config);
	    assertEquals(DecisionEngine.Strategy.VStore, de.getStrategy());
		
		config.getProperties().put("cserver.decision_engine", "hstore");
        de = DecisionEngine.createDecisionEngine(config);
        assertEquals(DecisionEngine.Strategy.HStore, de.getStrategy());
		
		config.getProperties().put("cserver.decision_engine", "fuck-show");
		boolean error = false;
		try {
		    de = DecisionEngine.createDecisionEngine(config);
		} catch(AssertionError e) {
			error = true;
		}
		assertTrue(error);

	}
	@Test
	public void testMakeDecision() {

		CServerConfig config = new CServerConfig();
        DecisionEngine de = DecisionEngine.createDecisionEngine(config);
		ScanRowsRequest request = new ScanRowsRequest();
		
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de.makeDecision(request));

        config.getProperties().put("cserver.decision_engine", "vstore");
        de = DecisionEngine.createDecisionEngine(config);
	    
	    assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(request));
        
        config.getProperties().put("cserver.decision_engine", "hstore");
        de = DecisionEngine.createDecisionEngine(config);

        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de.makeDecision(request));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de.makeDecision(request));
        
	}

}
