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

import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.ExternalRoutineInvocationException;
import com.foundationdb.server.error.NoSuchRoutineException;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.session.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.*;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class ScriptCache {
    private final DXLService dxlService;
    private final Map<TableName, CacheEntry> cache = new HashMap<>();
    // Script engine discovery can be fairly expensive, so it is deferred.
    private final ScriptEngineManagerProvider engineProvider;
    private final static Logger logger = LoggerFactory.getLogger(ScriptCache.class);

    public ScriptCache(DXLService dxlService, ScriptEngineManagerProvider engineProvider) {
        this.dxlService = dxlService;
        this.engineProvider = engineProvider;
    }

    public synchronized void clear() {
        cache.clear();
    }

    public synchronized void checkRemoveRoutine(TableName routineName, 
                                                long currentVersion) {
        CacheEntry entry = cache.remove(routineName);
        if ((entry != null) && (entry.version == currentVersion)) {
            cache.put(routineName, entry); // Was valid after all.
        }
    }

    public boolean isScriptLanguage(Session session, String language) {
        return (getManager(session).getEngineByName(language) != null);
    }

    public ScriptPool<ScriptEvaluator> getScriptEvaluator(Session session, TableName routineName) {
        return getEntry(session, routineName).getScriptEvaluator();
    }

    public ScriptPool<ScriptLibrary> getScriptLibrary(Session session, TableName routineName) {
        return getEntry(session, routineName).getScriptLibrary();
    }

    public ScriptPool<ScriptInvoker> getScriptInvoker(Session session, TableName routineName) {
        return getEntry(session, routineName).getScriptInvoker(this, session);
    }

    protected ScriptEngineManager getManager(Session session) {
        return engineProvider.getManager();
    }

    protected synchronized CacheEntry getEntry(Session session, TableName routineName) {
        Routine routine = dxlService.ddlFunctions().getAIS(session).getRoutine(routineName);
        if (null == routine)
            throw new NoSuchRoutineException(routineName);
        long currentVersion = routine.getVersion();
        CacheEntry entry = cache.get(routineName);
        if ((entry != null) && (entry.version == currentVersion)) 
            return entry;

        ClassLoader origCL = getContextClassLoader();
        
        if (!routine.isSystemRoutine()) {
            setContextClassLoader(engineProvider.getSafeClassLoader());
        }
        
        ScriptEngine engine = getManager(session).getEngineByName(routine.getLanguage());
        if (engine == null)
            throw new ExternalRoutineInvocationException(routineName, "Cannot find " + routine.getLanguage()
                    + " script engine");
        entry = new CacheEntry(routine, engine);
        cache.put(routineName, entry);
        if (!routine.isSystemRoutine()) {
            setContextClassLoader(origCL);
        }
        return entry;
    }
    
    private ClassLoader getContextClassLoader() {
        return AccessController.doPrivileged(
            new PrivilegedAction<ClassLoader>() {
                public ClassLoader run() {
                    return Thread.currentThread().getContextClassLoader();
                }
            }
        );        
    }
    
    private void setContextClassLoader(final ClassLoader cl) {
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        Thread.currentThread().setContextClassLoader(cl);
                        return null;
                    }
                }
        );
    }
    
    class CacheEntry {
        private TableName routineName;
        private long version;
        private String script;
        private TableName libraryName;
        private String function;
        private ScriptEngineFactory factory;
        private String threading;
        private boolean invocable, compilable;
        private ScriptPool<ScriptEvaluator> sharedEvaluatorPool;
        private ScriptPool<ScriptLibrary> sharedLibraryPool;
        private ScriptEngine spareEngine;

        public CacheEntry(Routine routine, ScriptEngine engine) {
            routineName = routine.getName();
            version = routine.getVersion();
            script = routine.getDefinition();
            libraryName = routineName; // TODO: Until qualified EXTERNAL NAME supported.
            function = routine.getMethodName();
            factory = engine.getFactory();
            threading = (String)factory.getParameter("THREADING");
            invocable = (engine instanceof Invocable);
            compilable = (engine instanceof Compilable);
            spareEngine = engine;
        }

        public ScriptPool<ScriptEvaluator> getScriptEvaluator() {
            // The only case where anything more than the factory is cached is
            // for THREAD-ISOLATED / STATELESS threading + Compilable,
            // which means that just one CompiledScript will work for
            // everyone.
            if (compilable && ("THREAD-ISOLATED".equals(threading) || "STATELESS".equals(threading))) {
                synchronized (this) {
                    if (sharedEvaluatorPool == null) {
                        ScriptEngine engine = spareEngine;
                        if (engine != null)
                            spareEngine = null;
                        else
                            engine = factory.getScriptEngine();
                        CompiledEvaluator compiled = new CompiledEvaluator(routineName, engine, script, true);
                        sharedEvaluatorPool = new SharedPool<ScriptEvaluator>(compiled);
                    }
                    return sharedEvaluatorPool;
                }
            }

            // Otherwise, every caller gets a new pool which only has
            // the scope of the prepared statement, etc.
            ScriptEngine engine;
            synchronized (this) {
                engine = spareEngine;
                if (engine != null)
                    spareEngine = null;
            }
            if (compilable) {
                CompiledEvaluator compiled = null;
                if (engine != null)
                    
                    compiled = new CompiledEvaluator(routineName, engine, script, false);
                return new CompiledEvaluatorPool(routineName, factory, script, compiled);
            } else {
                EngineEvaluator evaluator = null;
                if (engine != null)
                    evaluator = new EngineEvaluator(routineName, engine, script);
                return new EngineEvaluatorPool(routineName, factory, script, evaluator);
            }
        }

        public ScriptPool<ScriptLibrary> getScriptLibrary() {
            assert invocable;
            // Can share if at multi-threaded (or stronger), since we
            // are invoking the function.
            if ("MULTITHREADED".equals(threading) || 
                "THREAD-ISOLATED".equals(threading) || 
                "STATELESS".equals(threading)) {
                synchronized (this) {
                    if (sharedLibraryPool == null) {
                        ScriptEngine engine = spareEngine;
                        if (engine != null)
                            spareEngine = null;
                        else
                            engine = factory.getScriptEngine();
                        ScriptLibrary library = new Library(routineName, engine, script);
                        sharedLibraryPool = new SharedPool<>(library);
                    }
                    return sharedLibraryPool;
                }
            }

            // Otherwise, every caller gets a new pool which only has
            // the scope of the prepared statement, etc.
            ScriptEngine engine;
            synchronized (this) {
                engine = spareEngine;
                if (engine != null)
                    spareEngine = null;
            }
            Library library = null;
            if (engine != null)
                library = new Library(routineName, engine, script);
            return new LibraryPool(routineName, factory, script, library);
        }

        public ScriptPool<ScriptInvoker> getScriptInvoker(ScriptCache cache, Session session) {
            assert invocable && (function != null);
            ScriptPool<ScriptLibrary> libraryPool;
            if (routineName.equals(libraryName)) {
                libraryPool = getScriptLibrary();
            }
            else {
                synchronized (this) {
                    spareEngine = null;
                }
                libraryPool = cache.getScriptLibrary(session, libraryName);
            }
            return new InvokerPool(libraryPool, function);
        }
    }

    static class SharedPool<T> implements ScriptPool<T> {
        private final T instance;

        public SharedPool(T instance) {
            this.instance = instance;
        }

        @Override
        public T get() {
            return instance;
        }

        @Override
        public void put(T elem, boolean success) {
        }
    }

    // Relatively conservative, since these could be big.
    static final int FIXED_SIZE = 8;

    static abstract class FixedPool<T> implements ScriptPool<T> {
        private final Deque<T> pool = new ArrayDeque<>(FIXED_SIZE);

        public FixedPool(T initial) {
            if (initial != null)
                pool.addLast(initial);
        }

        protected abstract T create();

        @Override
        public T get() {
            T elem;
            synchronized (pool) {
                elem = pool.pollFirst();
            }
            if (elem != null)
                return elem;
            else
                return create();
        }

        @Override
        public void put(T elem, boolean success) {
            if (success) {
                synchronized (pool) {
                    pool.offerLast(elem);
                }
            }
        }
    }

    static abstract class BasePool<T> extends FixedPool<T> {
        protected final TableName routineName;
        protected final ScriptEngineFactory factory;
        protected final String script;

        public BasePool(TableName routineName, ScriptEngineFactory factory, String script, T initial) {
            super(initial);
            this.routineName = routineName;
            this.factory = factory;
            this.script = script;
        }
    }

    static class EngineEvaluatorPool extends BasePool<ScriptEvaluator> {
        public EngineEvaluatorPool(TableName routineName, ScriptEngineFactory factory, String script,
                EngineEvaluator initial) {
            super(routineName, factory, script, initial);
        }

        @Override
        protected EngineEvaluator create() {
            return new EngineEvaluator(routineName, factory.getScriptEngine(), script);
        }
    }

    static class CompiledEvaluatorPool extends BasePool<ScriptEvaluator> {
        public CompiledEvaluatorPool(TableName routineName, ScriptEngineFactory factory, String script,
                CompiledEvaluator initial) {
            super(routineName, factory, script, initial);
        }

        @Override
        protected CompiledEvaluator create() {
            return new CompiledEvaluator(routineName, factory.getScriptEngine(), script, false);
        }
    }

    static class LibraryPool extends BasePool<ScriptLibrary> {
        public LibraryPool(TableName routineName, ScriptEngineFactory factory, String script, Library initial) {
            super(routineName, factory, script, initial);
        }

        @Override
        protected Library create() {
            return new Library(routineName, factory.getScriptEngine(), script);
        }
    }

    protected static void setScriptName(TableName routineName, ScriptEngine engine) {
        engine.getContext().setAttribute(ScriptEngine.FILENAME, routineName.toString(), ScriptContext.ENGINE_SCOPE);
    }

    static class EngineEvaluator implements ScriptEvaluator {
        private final TableName routineName;
        private final ScriptEngine engine;
        private final String script;

        public EngineEvaluator(TableName routineName, ScriptEngine engine, String script) {
            this.routineName = routineName;
            this.engine = engine;
            this.script = script;
            setScriptName(routineName, engine);
        }

        @Override
        public String getEngineName() {
            return engine.getFactory().getEngineName();
        }

        @Override
        public boolean isCompiled() {
            return false;
        }

        @Override
        public boolean isShared() {
            return false;
        }

        @Override
        public Bindings getBindings() {
            return engine.getBindings(ScriptContext.ENGINE_SCOPE);
        }

        @Override
        public Object eval(Bindings bindings) {
            logger.debug("Evaluating {}", routineName);
            try {
                return engine.eval(script); // Bindings came from engine.
            } catch (ScriptException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
        }
    }

    static class CompiledEvaluator implements ScriptEvaluator {
        private final TableName routineName;
        private final CompiledScript compiled;
        private final boolean shared;

        public CompiledEvaluator(TableName routineName, ScriptEngine engine, String script, boolean shared) {
            this.routineName = routineName;
            setScriptName(routineName, engine);
            logger.debug("Compiling {}", routineName);
            try {
                compiled = ((Compilable) engine).compile(script);
            } catch (ScriptException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
            this.shared = shared;
        }

        @Override
        public String getEngineName() {
            return compiled.getEngine().getFactory().getEngineName();
        }

        @Override
        public boolean isCompiled() {
            return true;
        }

        @Override
        public boolean isShared() {
            return shared;
        }

        @Override
        public Bindings getBindings() {
            // Prefer to use the Bindings already in the engine
            // instead of a fresh one, since some engines (Jython, for
            // instance) do not do well with a dynamic set.
            if (shared)
                return compiled.getEngine().createBindings();
            else
                return compiled.getEngine().getBindings(ScriptContext.ENGINE_SCOPE);
        }

        @Override
        public Object eval(Bindings bindings) {
            logger.debug("Loading compiled {}", routineName);
            try {
                if (shared)
                    return compiled.eval(bindings);
                else
                    return compiled.eval();
            } catch (ScriptException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
        }
    }

    static class Library implements ScriptLibrary {
        private final TableName routineName;
        private final Invocable invocable;

        public Library(TableName routineName, ScriptEngine engine, String script) {
            this.routineName = routineName;
            setScriptName(routineName, engine);
            try {
                if (engine instanceof Compilable) {
                    logger.debug("Compiling and loading {}", routineName);
                    ((Compilable) engine).compile(script).eval();
                } else {
                    logger.debug("Evaluating {}", routineName);
                    engine.eval(script);
                }
            } catch (ScriptException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
            invocable = (Invocable) engine;
        }

        @Override
        public String getEngineName() {
            return ((ScriptEngine) invocable).getFactory().getEngineName();
        }

        @Override
        public boolean isCompiled() {
            return (invocable instanceof Compilable);
        }

        @Override
        public Object invoke(String function, Object[] args) {
            logger.debug("Calling {} in {}", function, routineName);
            try {
                return invocable.invokeFunction(function, args);
            } catch (ScriptException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            } catch (NoSuchMethodException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
        }
    }

    static class InvokerPool implements ScriptPool<ScriptInvoker> {
        private final ScriptPool<ScriptLibrary> libraryPool;
        private final String function;

        public InvokerPool(ScriptPool<ScriptLibrary> libraryPool, String function) {
            this.libraryPool = libraryPool;
            this.function = function;
        }

        @Override
        public ScriptInvoker get() {
            return new Invoker(libraryPool.get(), function);
        }

        @Override
        public void put(ScriptInvoker elem, boolean success) {
            libraryPool.put(elem.getLibrary(), success);
        }
    }

    static class Invoker implements ScriptInvoker {
        private final ScriptLibrary library;
        private final String function;

        public Invoker(ScriptLibrary library, String function) {
            this.library = library;
            this.function = function;
        }

        @Override
        public ScriptLibrary getLibrary() {
            return library;
        }

        @Override
        public String getFunctionName() {
            return function;
        }

        @Override
        public Object invoke(Object[] args) {
            return library.invoke(function, args);
        }
    }
}
