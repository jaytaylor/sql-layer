
package com.akiban.server.service.routines;

import com.akiban.ais.model.TableName;

import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.server.service.session.Session;
import java.lang.reflect.Method;

public interface RoutineLoader
{
    public ClassLoader loadSQLJJar(Session session, TableName jarName);
    public void unloadSQLJJar(Session session, TableName jarName);

    public LoadablePlan<?> loadLoadablePlan(Session session, TableName routineName);
    public Method loadJavaMethod(Session session, TableName routineName);
    public boolean isScriptLanguage(Session session, String language);
    public ScriptPool<ScriptEvaluator> getScriptEvaluator(Session session, TableName routineName);
    public ScriptPool<ScriptInvoker> getScriptInvoker(Session session, TableName routineName);
    public void unloadRoutine(Session session, TableName routineName);
}
