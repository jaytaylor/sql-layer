/**
 * 
 */
package com.akiban.cserver.decider;

import com.akiban.message.Request;

/**
 * @author percent
 *
 */
public class RoundRobin implements Decider {
	public RoundRobin() {
		current = 0;
	}
	
	/**
	 *  (non-Javadoc)
	 * @see com.akiban.cserver.decider.Decider#decide(com.akiban.message.Request)
	 */
	@Override
	public EngineType decide(Request r) {
		// TODO Auto-generated method stub
		EngineType et = Decider.EngineType.values()[current];
		current ++;
		assert !(current > Decider.EngineType.values().length); 
		if(current == Decider.EngineType.values().length) {
			current = 0;
		}
		return et;
	}
	
	public int current;	
}
