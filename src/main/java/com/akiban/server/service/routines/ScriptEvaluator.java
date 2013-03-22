
package com.akiban.server.service.routines;

import javax.script.Bindings;

public interface ScriptEvaluator
{
    public String getEngineName();
    public boolean isCompiled();
    public boolean isShared();
    public Bindings getBindings();
    public Object eval(Bindings bindings);
}
