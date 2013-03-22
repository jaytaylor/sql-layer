
package com.akiban.server.service.routines;

import com.akiban.ais.model.TableName;

import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.server.service.session.Session;
import java.lang.reflect.Method;

/** All this does is say that every language is a script language. */
public class MockRoutineLoader implements RoutineLoader
{
    @Override
    public ClassLoader loadSQLJJar(Session session, TableName jarName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unloadSQLJJar(Session session, TableName jarName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LoadablePlan<?> loadLoadablePlan(Session session, TableName routineName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Method loadJavaMethod(Session session, TableName routineName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isScriptLanguage(Session session, String language) {
        return true;
    }

    @Override
    public ScriptPool<ScriptEvaluator> getScriptEvaluator(Session session, TableName routineName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScriptPool<ScriptInvoker> getScriptInvoker(Session session, TableName routineName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unloadRoutine(Session session, TableName routineName) {
        throw new UnsupportedOperationException();
    }
}
