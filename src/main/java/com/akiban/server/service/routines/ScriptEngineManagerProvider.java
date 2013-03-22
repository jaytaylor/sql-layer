
package com.akiban.server.service.routines;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngineManager;

public interface ScriptEngineManagerProvider {
    public static final String CLASS_PATH = "akserver.routines.script_class_path";
    public ScriptEngineManager getManager();
}
