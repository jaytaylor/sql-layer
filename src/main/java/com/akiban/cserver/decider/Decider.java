/**
 * 
 */
package com.akiban.cserver.decider;
import com.akiban.cserver.message.ScanRowsRequest;
/**
 * @author percent
 *
 */
public interface Decider {
	public static enum RowCollectorType {VCollector, PersistitRowCollector};
	public RowCollectorType decide(ScanRowsRequest r);
}
