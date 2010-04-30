/**
 * 
 */
package com.akiban.cserver.decider;
import com.akiban.message.*;
/**
 * @author percent
 *
 */
public interface Decider {
	public static enum EngineType {VStore, HStore};
	public EngineType decide(Request r);
}
