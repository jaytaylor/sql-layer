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
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.ais.model.aisb2.NewRoutineBuilder;
import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.server.error.SQLJInstanceException;
import com.akiban.server.error.NoSuchSQLJJarException;
import com.akiban.server.error.NoSuchRoutineException;
import com.akiban.server.service.Service;
import com.akiban.server.store.AisHolder;
import com.akiban.server.store.SchemaManager;

import com.akiban.qp.loadableplan.std.DumpGroupLoadablePlan;
import com.akiban.qp.loadableplan.std.PersistitCLILoadablePlan;

import javax.inject.Inject;
import com.google.inject.Singleton;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Singleton
public final class RoutineLoaderImpl implements RoutineLoader, Service {

    private final AisHolder aisHolder;
    private final SchemaManager schemaManager;
    private final Map<TableName,ClassLoader> classLoaders = new HashMap<TableName,ClassLoader>();
    private final Map<TableName,LoadablePlan<?>> loadablePlans = new HashMap<TableName,LoadablePlan<?>>();
    private final Map<TableName,Method> javaMethods = new HashMap<TableName,Method>();

    @Inject @SuppressWarnings("unused")
    public RoutineLoaderImpl(AisHolder aisHolder, 
                             SchemaManager schemaManager) {
        this.aisHolder = aisHolder;
        this.schemaManager = schemaManager;
    }

    /* RoutineLoader */

    @Override
    public ClassLoader loadSQLJJar(TableName jarName) {
        if (jarName == null)
            return getClass().getClassLoader();
        synchronized (classLoaders) {
            ClassLoader loader = classLoaders.get(jarName);
            if (loader != null)
                return loader;

            SQLJJar sqljJar = aisHolder.getAis().getSQLJJar(jarName);
            if (sqljJar == null)
                throw new NoSuchSQLJJarException(jarName);
            loader = new URLClassLoader(new URL[] { sqljJar.getURL() });
            classLoaders.put(jarName, loader);
            return loader;
        }
    }

    @Override
    public void unloadSQLJJar(TableName jarName) {
        synchronized (classLoaders) {
            classLoaders.remove(jarName);
        }        
    }

    @Override
    public LoadablePlan<?> loadLoadablePlan(TableName routineName) {
        LoadablePlan<?> loadablePlan;
        synchronized (loadablePlans) {
            loadablePlan = loadablePlans.get(routineName);
            if (loadablePlan == null) {
                Routine routine = aisHolder.getAis().getRoutine(routineName);
                if (routine == null)
                    throw new NoSuchRoutineException(routineName);
                SQLJJar sqljJar = routine.getSQLJJar();
                if ((sqljJar == null) || 
                    (routine.getCallingConvention() != Routine.CallingConvention.LOADABLE_PLAN))
                    throw new SQLJInstanceException(routineName, "Routine was not loadable plan");
                ClassLoader classLoader = loadSQLJJar(sqljJar.getName());
                try {
                    loadablePlan = (LoadablePlan<?>)
                        Class.forName(routine.getClassName(), true, classLoader).newInstance();
                }
                catch (Exception ex) {
                    throw new SQLJInstanceException(routineName, ex);
                }
            }
        }
        synchronized (loadablePlan) {
            if (loadablePlan.ais() != aisHolder.getAis())
                loadablePlan.ais(aisHolder.getAis());
        }
        return loadablePlan;
    }

    @Override
    public Method loadJavaMethod(TableName routineName) {
    }

    @Override
    public void unloadRoutine(TableName routineName) {
        synchronized (loadablePlans) {
            loadablePlans.remove(routineName);
        }
        synchronized (javaMethods) {
            javaMethods.remove(routineName);
        }
    }

    /* Service */

    @Override
    public void start() {
        registerSystemProcedures();
    }

    @Override
    public void stop() {
        if (false)              // Only started once for server and AIS wiped for tests.
            unregisterSystemProcedures();
    }

    @Override
    public void crash() {
        stop();
    }

    public static final int IDENT_MAX = 128;
    public static final int PATH_MAX = 1024;
    public static final int COMMAND_MAX = 1024;

    private void registerSystemProcedures() {
        NewAISBuilder aisb = AISBBasedBuilder.create();

        aisb.defaultSchema(TableName.SYS_SCHEMA);
        aisb.procedure("dump_group")
            .language("java", Routine.CallingConvention.LOADABLE_PLAN)
            .paramStringIn("schema_name", IDENT_MAX)
            .paramStringIn("table_name", IDENT_MAX)
            .paramLongIn("insert_max_row_count")
            .externalName("com.akiban.qp.loadableplan.std.DumpGroupLoadablePlan");
        aisb.procedure("persistitcli")
            .language("java", Routine.CallingConvention.LOADABLE_PLAN)
            .paramStringIn("command", COMMAND_MAX)
            .externalName("com.akiban.qp.loadableplan.std.PersistitCLILoadablePlan");

        aisb.defaultSchema(TableName.SQLJ_SCHEMA);
        aisb.procedure("install_jar")
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("url", PATH_MAX)
            .paramStringIn("jar", PATH_MAX)
            .paramLongIn("deploy")
            .externalName("com.akiban.server.service.routines.SQLJJarRoutines", "install");
        aisb.procedure("replace_jar")
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("url", PATH_MAX)
            .paramStringIn("jar", PATH_MAX)
            .externalName("com.akiban.server.service.routines.SQLJJarRoutines", "replace");
        aisb.procedure("remove_jar")
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("jar", PATH_MAX)
            .paramLongIn("undeploy")
            .externalName("com.akiban.server.service.routines.SQLJJarRoutines", "remove");

        Collection<Routine> procs = aisb.ais().getRoutines().values();
        for (Routine proc : procs) {
            schemaManager.registerSystemRoutine(proc);
        }
    }

    private void unregisterSystemProcedures() {
        schemaManager.unRegisterSystemRoutine(new TableName(TableName.SYS_SCHEMA,
                                                            "dump_group"));
        schemaManager.unRegisterSystemRoutine(new TableName(TableName.SYS_SCHEMA,
                                                            "persistitcli"));
    }
}
