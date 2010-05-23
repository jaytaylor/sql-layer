/**
 * 
 */
package com.akiban.cserver.decider;

import com.akiban.cserver.message.ScanRowsRequest;

/**
 * @author percent
 *
 */
public class Monadic implements Decider {
	public Monadic(Decider.RowCollectorType type) {
		engineType = type;
	}
	
	/**
	 *  (non-Javadoc)
	 * @see com.akiban.cserver.decider.Decider#decide(com.akiban.message.ScanRowsRequest)
	 */
	@Override
	public RowCollectorType decide(ScanRowsRequest r) {
		return engineType;
	}
	
	public Decider.RowCollectorType engineType;
}
