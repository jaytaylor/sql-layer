package com.akiban.cserver.loader;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.store.Store;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.VStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.*;

public class BulkLoader
{
    public void run() throws java.lang.Exception
    {
        DB db = new DB(dbHost, dbPort, dbUser, dbPassword);
        if (taskGeneratorActions == null) {
            taskGeneratorActions = new MySQLTaskGeneratorActions(ais);
        }
        IdentityHashMap<UserTable, TableTasks> tableTasksMap =
            new TaskGenerator(this, taskGeneratorActions).generateTasks();
        DataGrouper dataGrouper = new DataGrouper(db, artifactsSchema);
        if (resume) {
            dataGrouper.resume();
        } else {
            dataGrouper.run(tableTasksMap);
        }
        if (null != persistitStore) {
            new PersistitLoader(persistitStore, db, ais).load(finalTasks(tableTasksMap));
        } else {
            new VerticalLoader(verticalStore, db, ais).load(finalTasks(tableTasksMap));
        }
        if (cleanup) {
            dataGrouper.deleteWorkArea();
        }
        logger.info("Loading complete");
    }

    // For testing
    public BulkLoader(PersistitStore persistitStore,
                      AkibaInformationSchema ais,
                      List<String> groups,
                      String artifactsSchema,
                      Map<String, String> sourceSchemas,
                      String dbHost,
                      int dbPort,
                      String dbUser,
                      String dbPassword) throws ClassNotFoundException, SQLException
    {
        this.persistitStore = persistitStore;
        this.verticalStore = null;
        this.ais = ais;
        this.groups = groups;
        this.artifactsSchema = artifactsSchema;
        this.sourceSchemas = sourceSchemas;
        this.dbHost = dbHost;
        this.dbUser = dbUser;
        this.dbPort = dbPort;
        this.dbPassword = dbPassword;
    }

    public BulkLoader(VStore verticalStore,
                      AkibaInformationSchema ais,
                      List<String> groups,
                      String artifactsSchema,
                      Map<String, String> sourceSchemas,
                      String dbHost,
                      int dbPort,
                      String dbUser,
                      String dbPassword) throws ClassNotFoundException, SQLException
    {
        this.persistitStore = null;
        this.verticalStore = verticalStore;
        this.ais = ais;
        this.groups = groups;
        this.artifactsSchema = artifactsSchema;
        this.sourceSchemas = sourceSchemas;
        this.dbHost = dbHost;
        this.dbUser = dbUser;
        this.dbPort = dbPort;
        this.dbPassword = dbPassword;
    }

    public BulkLoader(Store store,
                      AkibaInformationSchema ais,
                      List<String> groups,
                      String artifactsSchema,
                      Map<String, String> sourceSchemas,
                      String dbHost,
                      int dbPort,
                      String dbUser,
                      String dbPassword) throws ClassNotFoundException, SQLException
    {
        if (store instanceof PersistitStore) {
            this.persistitStore = (PersistitStore) store;
            this.verticalStore = null;
        } else {
            this.persistitStore = null;
            this.verticalStore = (VStore) store;
        }
        this.ais = ais;
        this.groups = groups;
        this.artifactsSchema = artifactsSchema;
        this.sourceSchemas = sourceSchemas;
        this.dbHost = dbHost;
        this.dbUser = dbUser;
        this.dbPort = dbPort;
        this.dbPassword = dbPassword;
    }

    String artifactsSchema()
    {
        return artifactsSchema;
    }

    String sourceSchema(String targetSchema)
    {
        String sourceSchema = sourceSchemas.get(targetSchema);
        assert sourceSchema != null : targetSchema;
        return sourceSchema;
    }

    List<String> groups()
    {
        return groups;
    }

    AkibaInformationSchema ais()
    {
        return ais;
    }

    private static List<GenerateFinalTask> finalTasks(IdentityHashMap<UserTable, TableTasks> tableTasksMap)
    {
        List<GenerateFinalTask> finalTasks = new ArrayList<GenerateFinalTask>();
        for (TableTasks tableTasks : tableTasksMap.values()) {
            GenerateFinalTask finalTask = tableTasks.generateFinal();
            if (finalTask != null) {
                finalTasks.add(finalTask);
            }
        }
        return finalTasks;
    }

    private static final Log logger = LogFactory.getLog(BulkLoader.class.getName());

    private boolean resume = false;
    private boolean cleanup = true;
    private String dbHost;
    private int dbPort;
    private String dbUser;
    private String dbPassword;
    private String artifactsSchema;
    private List<String> groups;
    private Map<String, String> sourceSchemas;
    private PersistitStore persistitStore;
    private VStore verticalStore;
    private AkibaInformationSchema ais;
    private TaskGenerator.Actions taskGeneratorActions;

    public static class RuntimeException extends java.lang.RuntimeException
    {
        RuntimeException(String message)
        {
            super(message);
        }

        RuntimeException(String message, Throwable th)
        {
            super(message, th);
        }
    }

    public static class InternalError extends java.lang.Error
    {
        InternalError(String message)
        {
            super(message);
        }
    }
}
