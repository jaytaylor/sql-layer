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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.ais.protobuf.*;
import com.foundationdb.qp.loadableplan.LoadablePlan;
import com.foundationdb.qp.loadableplan.std.DumpGroupLoadablePlan;
import com.foundationdb.qp.loadableplan.std.GroupProtobufLoadablePlan;
import com.foundationdb.server.error.SQLJInstanceException;
import com.foundationdb.server.error.NoSuchSQLJJarException;
import com.foundationdb.server.error.NoSuchRoutineException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.service.blob.LobRoutines;

import com.google.inject.Singleton;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarFile;

@Singleton
public final class RoutineLoaderImpl implements RoutineLoader, Service {

    static class VersionedItem<T> {
        long version;
        T item;

        VersionedItem(long version, T item) {
            this.version = version;
            this.item = item;
        }
    }

    private final DXLService dxlService;
    private final SchemaManager schemaManager;
    private final Map<TableName,VersionedItem<ClassLoader>> classLoaders = new HashMap<>();
    private final Map<TableName,VersionedItem<LoadablePlan<?>>> loadablePlans = new HashMap<>();
    private final Map<TableName,VersionedItem<Method>> javaMethods = new HashMap<>();
    private final ScriptCache scripts;
    private final static Logger logger = LoggerFactory.getLogger(RoutineLoaderImpl.class);

    @Inject @SuppressWarnings("unused")
    public RoutineLoaderImpl(DXLService dxlService,
                             SchemaManager schemaManager,
                             ConfigurationService configService,
                             ScriptEngineManagerProvider engineProvider) {
        this.dxlService = dxlService;
        this.schemaManager = schemaManager;
        scripts = new ScriptCache(dxlService, engineProvider);
    }

    private AkibanInformationSchema ais(Session session) {
        return dxlService.ddlFunctions().getAIS(session);
    }

    /* RoutineLoader */

    @Override
    public ClassLoader loadSQLJJar(Session session, TableName jarName) {
        if (jarName == null)
            return getClass().getClassLoader();
        SQLJJar sqljJar = ais(session).getSQLJJar(jarName);
        if (sqljJar == null)
            throw new NoSuchSQLJJarException(jarName);
        long currentVersion = sqljJar.getVersion();
        synchronized (classLoaders) {
            VersionedItem<ClassLoader> entry = classLoaders.get(jarName);
            if ((entry != null) && (entry.version == currentVersion))
                return entry.item;
            ClassLoader loader = new URLClassLoader(new URL[] { sqljJar.getURL() });
            if (entry != null) {
                entry.item = loader;
            }
            else {
                entry = new VersionedItem<>(currentVersion, loader);
                classLoaders.put(jarName, entry);
            }
            return loader;
        }
    }

    @Override
    public void checkUnloadSQLJJar(Session session, TableName jarName) {
        SQLJJar sqljJar = ais(session).getSQLJJar(jarName);
        long currentVersion = -1;
        if (sqljJar != null)
            currentVersion = sqljJar.getVersion();
        synchronized (classLoaders) {
            VersionedItem<ClassLoader> entry = classLoaders.remove(jarName);
            if ((entry != null) && (entry.version == currentVersion)) {
                classLoaders.put(jarName, entry); // Was valid after all.
            }
        }        
    }

    @Override
    public void registerSystemSQLJJar(SQLJJar sqljJar, ClassLoader classLoader) {
        TableName jarName = sqljJar.getName();
        long currentVersion = sqljJar.getVersion();
        synchronized (classLoaders) {
            VersionedItem<ClassLoader> entry = classLoaders.get(jarName);
            if (entry != null) {
                entry.version = currentVersion;
                entry.item = classLoader;
            }
            else {
                entry = new VersionedItem<>(currentVersion, classLoader);
                classLoaders.put(jarName, entry);
            }
        }
    }

    @Override
    public JarFile openSQLJJarFile(Session session, TableName jarName) throws IOException {
        SQLJJar sqljJar = ais(session).getSQLJJar(jarName);
        if (sqljJar == null)
            throw new NoSuchSQLJJarException(jarName);
        URL jarURL = new URL("jar:" + sqljJar.getURL() + "!/");
        return ((JarURLConnection)jarURL.openConnection()).getJarFile();
    }

    @Override
    public LoadablePlan<?> loadLoadablePlan(Session session, TableName routineName) {
        AkibanInformationSchema ais = ais(session);
        Routine routine = ais.getRoutine(routineName);
        if (routine == null)
            throw new NoSuchRoutineException(routineName);
        if (routine.getCallingConvention() != Routine.CallingConvention.LOADABLE_PLAN)
            throw new SQLJInstanceException(routineName, "Routine was not loadable plan");
        long currentVersion = routine.getVersion();
        LoadablePlan<?> loadablePlan;
        synchronized (loadablePlans) {
            VersionedItem<LoadablePlan<?>> entry = loadablePlans.get(routineName);
            if ((entry != null) && (entry.version == currentVersion)) {
                loadablePlan = entry.item;
            }
            else {
                TableName jarName = null;
                if (routine.getSQLJJar() != null)
                    jarName = routine.getSQLJJar().getName();
                ClassLoader classLoader = loadSQLJJar(session, jarName);
                try {
                    loadablePlan = (LoadablePlan<?>)
                        Class.forName(routine.getClassName(), true, classLoader).newInstance();
                }
                catch (Exception ex) {
                    throw new SQLJInstanceException(routineName, ex);
                }
                if (entry != null) {
                    entry.item = loadablePlan;
                }
                else {
                    entry = new VersionedItem<LoadablePlan<?>>(currentVersion, loadablePlan);
                    loadablePlans.put(routineName, entry);
                }
            }
        }
        synchronized (loadablePlan) {
            if (loadablePlan.ais() != ais)
                loadablePlan.ais(ais);
        }
        return loadablePlan;
    }

    @Override
    public Method loadJavaMethod(Session session, TableName routineName) {
        Routine routine = ais(session).getRoutine(routineName);
        if (routine == null)
            throw new NoSuchRoutineException(routineName);
        if (routine.getCallingConvention() != Routine.CallingConvention.JAVA)
            throw new SQLJInstanceException(routineName, "Routine was not SQL/J");
        long currentVersion = routine.getVersion();
        synchronized (javaMethods) {
            VersionedItem<Method> entry = javaMethods.get(routineName);
            if ((entry != null) && (entry.version == currentVersion))
                return entry.item;
            TableName jarName = null;
            if (routine.getSQLJJar() != null)
                jarName = routine.getSQLJJar().getName();
            ClassLoader classLoader = loadSQLJJar(session, jarName);
            Class<?> clazz;
            try {
                clazz = Class.forName(routine.getClassName(), true, classLoader);
            }
            catch (Exception ex) {
                throw new SQLJInstanceException(routineName, ex);
            }
            String methodName = routine.getMethodName();
            String methodArgs = null;
            int idx = methodName.indexOf('(');
            if (idx >= 0) {
                methodArgs = methodName.substring(idx+1);
                methodName = methodName.substring(0, idx);
            }
            Method javaMethod = null;
            for (Method method : clazz.getMethods()) {
                if (((method.getModifiers() & (Modifier.PUBLIC | Modifier.STATIC)) ==
                                              (Modifier.PUBLIC | Modifier.STATIC)) &&
                    method.getName().equals(methodName) &&
                    ((methodArgs == null) ||
                     (method.toString().indexOf(methodArgs) > 0))) {
                    javaMethod = method;
                    break;
                }
            }
            if (javaMethod == null)
                throw new SQLJInstanceException(routineName, "Method not found");
            if (entry != null) {
                entry.item = javaMethod;
            }
            else {
                entry = new VersionedItem<>(currentVersion, javaMethod);
                javaMethods.put(routineName, entry);
            }
            return javaMethod;
        }
    }

    @Override
    public boolean isScriptLanguage(Session session, String language) {
        return scripts.isScriptLanguage(session, language);
    }

    @Override
    public ScriptPool<ScriptEvaluator> getScriptEvaluator(Session session, TableName routineName) {
        return scripts.getScriptEvaluator(session, routineName);
    }

    @Override
    public ScriptPool<ScriptInvoker> getScriptInvoker(Session session, TableName routineName) {
        return scripts.getScriptInvoker(session, routineName);
    }

    @Override
    public ScriptPool<ScriptLibrary> getScriptLibrary(Session session, TableName routineName) {
        return scripts.getScriptLibrary(session, routineName);
    }

    @Override
    public void checkUnloadRoutine(Session session, TableName routineName) {
        Routine routine = ais(session).getRoutine(routineName);
        long currentVersion = -1;
        if (routine != null)
            currentVersion = routine.getVersion();
        synchronized (loadablePlans) {
            VersionedItem<LoadablePlan<?>> entry = loadablePlans.remove(routineName);
            if ((entry != null) && (entry.version == currentVersion)) {
                loadablePlans.put(routineName, entry); // Was valid after all.
            }
        }
        synchronized (javaMethods) {
            VersionedItem<Method> entry = javaMethods.remove(routineName);
            if ((entry != null) && (entry.version == currentVersion)) {
                javaMethods.put(routineName, entry);
            }
        }
        scripts.checkRemoveRoutine(routineName, currentVersion);
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

    private void registerSystemProcedures() {
        NewAISBuilder aisb = AISBBasedBuilder.create(schemaManager.getTypesTranslator());

        aisb.defaultSchema(TableName.SYS_SCHEMA);
        aisb.procedure("dump_group")
            .language("java", Routine.CallingConvention.LOADABLE_PLAN)
            .paramStringIn("schema_name", IDENT_MAX)
            .paramStringIn("table_name", IDENT_MAX)
            .paramLongIn("insert_max_row_count")
            .externalName(DumpGroupLoadablePlan.class.getCanonicalName());
        aisb.procedure("group_protobuf")
            .language("java", Routine.CallingConvention.LOADABLE_PLAN)
            .paramStringIn("schema_name", IDENT_MAX)
            .paramStringIn("table_name", IDENT_MAX)
            .externalName(GroupProtobufLoadablePlan.class.getCanonicalName());

        // Query logging
        aisb.procedure("query_log_set_enabled")
            .language("java", Routine.CallingConvention.JAVA)
            .paramBooleanIn("enabled")
            .externalName(QueryLoggingRoutines.class.getCanonicalName(), "setEnabled");
        aisb.procedure("query_log_is_enabled")
            .language("java", Routine.CallingConvention.JAVA)
            .returnBoolean("is_enabled")
            .externalName(QueryLoggingRoutines.class.getCanonicalName(), "isEnabled");
        aisb.procedure("query_log_set_file")
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("filename", PATH_MAX)
            .externalName(QueryLoggingRoutines.class.getCanonicalName(), "setFile");
        aisb.procedure("query_log_get_file")
            .language("java", Routine.CallingConvention.JAVA)
            .returnString("filename", PATH_MAX)
            .externalName(QueryLoggingRoutines.class.getCanonicalName(), "getFile");
        aisb.procedure("query_log_set_millis")
            .language("java", Routine.CallingConvention.JAVA)
            .paramLongIn("milliseconds")
            .externalName(QueryLoggingRoutines.class.getCanonicalName(), "setMillis");
        aisb.procedure("query_log_get_millis")
            .language("java", Routine.CallingConvention.JAVA)
            .returnLong("milliseconds")
            .externalName(QueryLoggingRoutines.class.getCanonicalName(), "getMillis");

        aisb.defaultSchema(TableName.SQLJ_SCHEMA);
        aisb.procedure("install_jar")
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("url", PATH_MAX)
            .paramStringIn("jar", PATH_MAX)
            .paramLongIn("deploy")
            .externalName(SQLJJarRoutines.class.getCanonicalName(), "install");
        aisb.procedure("replace_jar")
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("url", PATH_MAX)
            .paramStringIn("jar", PATH_MAX)
            .externalName(SQLJJarRoutines.class.getCanonicalName(), "replace");
        aisb.procedure("remove_jar")
            .language("java", Routine.CallingConvention.JAVA)
            .paramStringIn("jar", PATH_MAX)
            .paramLongIn("undeploy")
            .externalName(SQLJJarRoutines.class.getCanonicalName(), "remove");
        
        aisb.defaultSchema(TableName.SYS_SCHEMA);
        aisb.procedure("create_new_lob")
            .language("java", Routine.CallingConvention.JAVA)
            .returnString("lob_id", 36)
            .externalName(LobRoutines.class.getCanonicalName(), "createNewLob");
        aisb.procedure("read_blob")
                .language("java", Routine.CallingConvention.JAVA)
                .paramLongIn("offset")
                .paramIntegerIn("length")
                .paramStringIn("schema", PATH_MAX)
                .paramStringIn("blob_id", PATH_MAX)
                .returnVarBinary("data", Integer.MAX_VALUE)
                .externalName(LobRoutines.class.getCanonicalName(), "readBlob");
        aisb.procedure("read_in_table_blob")
                .language("java", Routine.CallingConvention.JAVA)
                .paramLongIn("offset")
                .paramIntegerIn("length")
                .paramStringIn("schema", PATH_MAX)
                .paramStringIn("table", PATH_MAX)
                .paramStringIn("column", PATH_MAX)
                .paramStringIn("blob_id", PATH_MAX)
                .returnVarBinary("data", Integer.MAX_VALUE)
                .externalName(LobRoutines.class.getCanonicalName(), "readBlobInTable");
        aisb.procedure("write_blob")
                .language("java", Routine.CallingConvention.JAVA)
                .paramLongIn("offset")
                .paramVarBinaryIn("data", Integer.MAX_VALUE)
                .paramStringIn("schema", PATH_MAX)
                .paramStringIn("blob_id", PATH_MAX)
                .externalName(LobRoutines.class.getCanonicalName(), "writeBlob");
        aisb.procedure("append_blob")
                .language("java", Routine.CallingConvention.JAVA)
                .paramVarBinaryIn("data", Integer.MAX_VALUE)
                .paramStringIn("schema", PATH_MAX)
                .paramStringIn("blob_id", PATH_MAX)
                .externalName(LobRoutines.class.getCanonicalName(), "appendBlob");
        aisb.procedure("truncate_blob")
                .language("java", Routine.CallingConvention.JAVA)
                .paramLongIn("length")
                .paramStringIn("schema", PATH_MAX)
                .paramStringIn("blob_id", PATH_MAX)
                .externalName(LobRoutines.class.getCanonicalName(), "truncateBlob");
        aisb.procedure("move_blob")
                .language("java", Routine.CallingConvention.JAVA)
                .paramStringIn("schema", PATH_MAX)
                .paramStringIn("blob_id", PATH_MAX)
                .paramStringIn("new_schema", PATH_MAX)
                .paramStringIn("new_table", PATH_MAX)
                .paramStringIn("new_column", PATH_MAX)
                .externalName(LobRoutines.class.getCanonicalName(), "moveBlob");
        aisb.procedure("delete_blob")
                .language("java", Routine.CallingConvention.JAVA)
                .paramStringIn("schema", PATH_MAX)
                .paramStringIn("blob_id", PATH_MAX)
                .externalName(LobRoutines.class.getCanonicalName(), "deleteBlob");
        aisb.procedure("delete_in_table_blob")
                .language("java", Routine.CallingConvention.JAVA)
                .paramStringIn("schema", PATH_MAX)
                .paramStringIn("new_table", PATH_MAX)
                .paramStringIn("new_column", PATH_MAX)
                .paramStringIn("blob_id", PATH_MAX)
                .externalName(LobRoutines.class.getCanonicalName(), "deleteBlobInTable");
        
        Collection<Routine> procs = aisb.ais().getRoutines().values();
        for (Routine proc : procs) {
            schemaManager.registerSystemRoutine(proc);
        }
    }

    private void unregisterSystemProcedures() {
        schemaManager.unRegisterSystemRoutine(new TableName(TableName.SYS_SCHEMA,
                                                            "dump_group"));
    }
}
