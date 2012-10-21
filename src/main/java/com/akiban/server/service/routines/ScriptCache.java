/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.service.routines;

import com.akiban.ais.model.Routine;
import com.akiban.ais.model.TableName;
import com.akiban.server.error.ExternalRoutineInvocationException;
import com.akiban.server.error.NoSuchRoutineException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.SchemaManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.*;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class ScriptCache
{
    public static final String CLASS_PATH = "akserver.routines.script_class_path";
    private final SchemaManager schemaManager;
    private final ConfigurationService configService;
    private final Map<TableName,CacheEntry> cache = new HashMap<TableName,CacheEntry>();
    // Script engine discovery can be fairly expensive, so it is deferred.
    private ScriptEngineManager manager = null;
    private final static Logger logger = LoggerFactory.getLogger(ScriptCache.class);

    public ScriptCache(SchemaManager schemaManager, ConfigurationService configService) {
        this.schemaManager = schemaManager;
        this.configService = configService;
    }

    public synchronized void clear() {
        cache.clear();
    }

    public synchronized void remove(TableName routineName) {
        cache.remove(routineName);
    }
    
    public boolean isScriptLanguage(Session session, String language) {
        return (getManager(session).getEngineByName(language) != null);
    }

    public ScriptPool<ScriptEvaluator> getScriptEvaluator(Session session, TableName routineName) {
        return getEntry(session, routineName).getScriptEvaluator();
    }

    public ScriptPool<ScriptInvoker> getScriptInvoker(Session session, TableName routineName) {
        return getEntry(session, routineName).getScriptInvoker();
    }
    
    protected ScriptEngineManager getManager(Session session) {
        if (manager == null) {
            logger.debug("Initializing script engine manager");
            String classPath = configService.getProperty(CLASS_PATH);
            // TODO: Everything is in the boot class loader presently, but the idea
            // is to restrict scripts to standard Java classes without
            // the rest of the Akiban server.
            ClassLoader classLoader = java.math.BigDecimal.class.getClassLoader();
            if (!classPath.equals("")) {
                try {
                    String[] paths = classPath.split(File.pathSeparator);
                    URL[] urls = new URL[paths.length];
                    for (int i = 0; i < paths.length; i++) {
                        urls[i] = new File(paths[i]).toURL();
                    }
                    System.out.println("??? " + java.util.Arrays.toString(urls));
                    classLoader = new URLClassLoader(urls, classLoader);
                }
                catch (MalformedURLException ex) {
                    logger.warn("Error setting script class loader", ex);
                }
            }
            synchronized (this) {
                if (manager == null)
                    manager = new ScriptEngineManager(classLoader);
            }
        }
        return manager;
    }

    protected synchronized CacheEntry getEntry(Session session, TableName routineName) {
        CacheEntry entry = cache.get(routineName);
        if (entry != null)
            return entry;
        Routine routine = schemaManager.getAis(session).getRoutine(routineName);
        if (null == routine)
            throw new NoSuchRoutineException(routineName);
        ScriptEngine engine = getManager(session).getEngineByName(routine.getLanguage());
        if (engine == null)
            throw new ExternalRoutineInvocationException(routineName,
                                                         "Cannot find " + routine.getLanguage() + " script engine");
        entry = new CacheEntry(routine, engine);
        cache.put(routineName, entry);
        return entry;
    }

    static class CacheEntry {
        private TableName routineName;
        private String script;
        private String function;
        private ScriptEngineFactory factory;
        private String threading;
        private boolean invocable, compilable;
        private ScriptPool<ScriptEvaluator> sharedEvaluatorPool;
        private ScriptPool<ScriptInvoker> sharedInvokerPool;
        private ScriptEngine spareEngine;

        public CacheEntry(Routine routine, ScriptEngine engine) {
            routineName = routine.getName();
            script = routine.getDefinition();
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
            if (compilable &&
                ("THREAD-ISOLATED".equals(threading) || "STATELESS".equals(threading))) {
                synchronized (this) {
                    if (sharedEvaluatorPool == null) {
                        ScriptEngine engine = spareEngine;
                        if (engine != null)
                            spareEngine = null;
                        else
                            engine = factory.getScriptEngine();
                        CompiledEvaluator compiled = new CompiledEvaluator(routineName,
                                                                           engine,
                                                                           script);
                        sharedEvaluatorPool = new SharedPool<ScriptEvaluator>(compiled);
                    }
                    return sharedEvaluatorPool;
                }
            }

            // Otherwise, every caller gets a new pool which only has the scope of the
            // prepared statement, etc.
            ScriptEngine engine;
            synchronized (this) {
                engine = spareEngine;
                if (engine != null)
                    spareEngine = null;
            }
            if (compilable) {
                CompiledEvaluator compiled = null;
                if (engine != null)
                    compiled = new CompiledEvaluator(routineName, engine, script);
                return new CompiledEvaluatorPool(routineName, factory, script, compiled);
            }
            else {
                EngineEvaluator evaluator = null;
                if (engine != null)
                    evaluator = new EngineEvaluator(routineName, engine, script);
                return new EngineEvaluatorPool(routineName, factory, script, evaluator);
            }
        }

        public ScriptPool<ScriptInvoker> getScriptInvoker() {
            assert invocable && (function != null);
            // Can share if at multi-threaded (or stronger), since we are invoking
            // the function.
            if ("MULTITHREADED".equals(threading) ||
                "THREAD-ISOLATED".equals(threading) ||
                "STATELESS".equals(threading)) {
                synchronized (this) {
                    if (sharedInvokerPool == null) {
                        ScriptEngine engine = spareEngine;
                        if (engine != null)
                            spareEngine = null;
                        else
                            engine = factory.getScriptEngine();
                        ScriptInvoker invoker = new Invoker(routineName, 
                                                            engine, script, function);
                        sharedInvokerPool = new SharedPool<ScriptInvoker>(invoker);
                    }
                    return sharedInvokerPool;
                }                
            }

            // Otherwise, every caller gets a new pool which only has the scope of the
            // prepared statement, etc.
            ScriptEngine engine;
            synchronized (this) {
                engine = spareEngine;
                if (engine != null)
                    spareEngine = null;
            }
            Invoker invoker = null;
            if (engine != null)
                invoker = new Invoker(routineName, engine, script, function);
            return new InvokerPool(routineName, factory, script, function, invoker);
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
        private final Deque<T> pool = new ArrayDeque<T>(FIXED_SIZE);

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
        public EngineEvaluatorPool(TableName routineName, ScriptEngineFactory factory, String script, EngineEvaluator initial) {
            super(routineName, factory, script, initial);
        }

        @Override
        protected EngineEvaluator create() {
            return new EngineEvaluator(routineName, factory.getScriptEngine(), script);
        }
    }

    static class CompiledEvaluatorPool extends BasePool<ScriptEvaluator> {
        public CompiledEvaluatorPool(TableName routineName, ScriptEngineFactory factory, String script, CompiledEvaluator initial) {
            super(routineName, factory, script, initial);
        }

        @Override
        protected CompiledEvaluator create() {
            return new CompiledEvaluator(routineName, factory.getScriptEngine(), script);
        }
    }

    static class InvokerPool extends BasePool<ScriptInvoker> {
        private final String function;
        
        public InvokerPool(TableName routineName, ScriptEngineFactory factory, String script, String function, Invoker initial) {
            super(routineName, factory, script, initial);
            this.function = function;
        }

        @Override
        protected Invoker create() {
            return new Invoker(routineName, factory.getScriptEngine(), script, function);
        }
    }

    static class EngineEvaluator implements ScriptEvaluator {
        private final TableName routineName;
        private final ScriptEngine engine;
        private final String script;

        public EngineEvaluator(TableName routineName, ScriptEngine engine, String script) {
            this.routineName = routineName;
            this.engine = engine;
            this.script = script;
        }

        @Override
        public Bindings createBindings() {
            return engine.createBindings();
        }

        @Override
        public Object eval(Bindings bindings) {
            logger.debug("Evaluating {}", routineName);
            try {
                return engine.eval(script, bindings);
            }
            catch (ScriptException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
        }
    }

    static class CompiledEvaluator implements ScriptEvaluator {
        private final TableName routineName;
        private final CompiledScript compiled;

        public CompiledEvaluator(TableName routineName, ScriptEngine engine, String script) {
            this.routineName = routineName;
            logger.debug("Compiling {}", routineName);
            try {
                compiled = ((Compilable)engine).compile(script);
            }
            catch (ScriptException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
        }

        @Override
        public Bindings createBindings() {
            return compiled.getEngine().createBindings();
        }

        @Override
        public Object eval(Bindings bindings) {
            logger.debug("Loading compiled {}", routineName);
            try {
                return compiled.eval(bindings);
            }
            catch (ScriptException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
        }
    }

    static class Invoker implements ScriptInvoker {
        private final TableName routineName;
        private final String function;
        private final Invocable invocable;
        
        public Invoker(TableName routineName, ScriptEngine engine, 
                       String script, String function) {
            this.routineName = routineName;
            this.function = function;
            try {
                if (engine instanceof Compilable) {
                    logger.debug("Compiling and loading {}", routineName);
                    ((Compilable)engine).compile(script).eval();
                }
                else {
                    logger.debug("Evaluating {}", routineName);
                    engine.eval(script);
                }
            }
            catch (ScriptException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
            invocable = (Invocable)engine;
        }

        @Override
        public Object invoke(Object[] args) {
            logger.debug("Calling {} in {}", function, routineName);
            try {
                return invocable.invokeFunction(function, args);
            }
            catch (ScriptException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
            catch (NoSuchMethodException ex) {
                throw new ExternalRoutineInvocationException(routineName, ex);
            }
        }
    }
}
