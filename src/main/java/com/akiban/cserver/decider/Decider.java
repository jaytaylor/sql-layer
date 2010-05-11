/**
 * 
 */
package com.akiban.cserver.decider;
import com.akiban.message.Request;
/**
 * @author percent
 *
 */
public interface Decider {
	public static enum EngineType {VStore, HStore};
	public EngineType decide(Request r);
}
