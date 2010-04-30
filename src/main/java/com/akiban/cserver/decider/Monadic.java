/**
 * 
 */
package com.akiban.cserver.decider;

import com.akiban.message.Request;

/**
 * @author percent
 *
 */
public class Monadic implements Decider {
	public Monadic(Decider.EngineType type) {
		engineType = type;
	}
	
	/**
	 *  (non-Javadoc)
	 * @see com.akiban.cserver.decider.Decider#decide(com.akiban.message.Request)
	 */
	@Override
	public EngineType decide(Request r) {
		return engineType;
	}
	
	public Decider.EngineType engineType;
}
