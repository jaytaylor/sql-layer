/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.routines;

import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.TableName;

import com.foundationdb.qp.loadableplan.LoadablePlan;
import com.foundationdb.server.service.session.Session;
import java.lang.reflect.Method;
import java.util.jar.JarFile;

/** All this does is say that every language is a script language. */
public class MockRoutineLoader implements RoutineLoader
{
    @Override
    public ClassLoader loadSQLJJar(Session session, TableName jarName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkUnloadSQLJJar(Session session, TableName jarName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerSystemSQLJJar(SQLJJar sqljJar, ClassLoader classLoader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JarFile openSQLJJarFile(Session session, TableName jarName) {
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
    public ScriptPool<ScriptLibrary> getScriptLibrary(Session session, TableName routineName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkUnloadRoutine(Session session, TableName routineName) {
        throw new UnsupportedOperationException();
    }
}
