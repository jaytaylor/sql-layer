/**
 * 
 */
package com.akiban.cserver.decider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.cserver.CServerConfig;

/**
 * @author percent
 */

public class Factory {
	public static enum DeciderType {HStore, VStore, RR};	
	public static Factory getSingleton() {
		if(SINGLETON == null) {
			SINGLETON = new Factory();
		}		
		return SINGLETON;
	}

	public static void setConfig(CServerConfig config) {
		CONFIG = config;
		if(SINGLETON != null) {
			SINGLETON.configure();
		}
	}
	
	public DeciderType getType() {
		return type;
	}

	public Decider createDecisionEngine() {
		Decider ret = null;
		switch(type) {
		case RR:
			ret = new RoundRobin();
			break;
			
		case VStore:
			ret = new Monadic(Decider.EngineType.VStore);
			break;
			
		case HStore:
			ret = new Monadic(Decider.EngineType.HStore);
			break;
	
		default:
			assert false;
		}
		assert ret != null;
		return ret;
	}

	protected Factory() {
		configure();
	}

	protected void configure() {
		type = Factory.DeciderType.RR;
		if(CONFIG != null) {
			final String value = CONFIG.property(DECISION_ENGINE);
			if(value != null) {
				if(value.equals("hstore")) {
					type = Factory.DeciderType.HStore;
				} else if(value.equals("vstore")) {
					type = Factory.DeciderType.VStore;
				} else {
					assert false;
				}
			}
		}
	}
	
	private static Factory SINGLETON=null;
	private static CServerConfig CONFIG=null;
	private static final Log LOG = LogFactory.getLog(Factory.class.getName());
	private static final String DECISION_ENGINE = "cserver.decision_engine";
	private DeciderType type;
}
