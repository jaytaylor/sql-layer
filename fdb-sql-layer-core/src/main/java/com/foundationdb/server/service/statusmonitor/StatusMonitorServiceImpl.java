/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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
package com.foundationdb.server.service.statusmonitor;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.externaldata.GenericRowTracker;
import com.foundationdb.server.service.externaldata.JsonRowWriter;
import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.sql.Main;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.sql.embedded.JDBCDriver;
import com.foundationdb.sql.embedded.JDBCResultSet;
import com.foundationdb.sql.embedded.JDBCResultSetMetaData;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.JsonUtils;
import com.google.inject.Inject;

public class StatusMonitorServiceImpl implements StatusMonitorService, Service {

    private final ConfigurationService configService;
    private final FDBHolder fdbService;
    private final EmbeddedJDBCService jdbcService;
    private static final Logger logger = LoggerFactory.getLogger(StatusMonitorServiceImpl.class);

    public static final List<String> STATUS_MONITOR_DIR = Arrays.asList("Status Monitor","Layers");
    public static final String STATUS_MONITOR_LAYER_NAME = "SQL Layer";
    
    public static final String CONFIG_FLUSH_INTERVAL = "fdbsql.fdb.status.flush_interval";
    public static final String CONFIG_STATUS_ENABLE = "fdbsql.fdb.status.enabled";

    protected byte[] instanceKey;
    private String host;
    private String port;
    protected Thread backgroundThread;
    protected volatile boolean running, backgroundIdle;
    private long flushInterval;
    private Future<Void> instanceWatch;
    FormatOptions options;
    
    
    @Inject
    public StatusMonitorServiceImpl (ConfigurationService configService, 
            FDBHolder fdbService,
            EmbeddedJDBCService jdbcService) {
        this.configService= configService;
        this.fdbService = fdbService;
        this.jdbcService = jdbcService;
        this.options = new FormatOptions();
        
    }
    
    @Override
    public void start() {
        // If not enabled (e.g. during testing), turn off the service. 
        if (!Boolean.parseBoolean(configService.getProperty(CONFIG_STATUS_ENABLE))) {
            return;
        }
        flushInterval = Long.parseLong(configService.getProperty(CONFIG_FLUSH_INTERVAL));
        options.set(FormatOptions.JsonBinaryFormatOption.fromProperty(configService.getProperty("fdbsql.sql.jsonbinary_output")));

        host = "127.0.0.1";
        try {
            host = InetAddress.getLocalHost().getHostName();
        }
        catch (IOException ex) {
            // Ignore
        }
        port = configService.getProperty("fdbsql.postgres.port");
        String address = host + ":" + port;

        DirectorySubspace rootDirectory = new DirectoryLayer().createOrOpen(fdbService.getTransactionContext(), STATUS_MONITOR_DIR).get();
        instanceKey = ByteArrayUtil.join(rootDirectory.pack(),
                Tuple2.from(STATUS_MONITOR_LAYER_NAME, address).pack());

        backgroundThread = new Thread(null, null, "Status Monitor Background") {
            @Override
            public void run() {
                backgroundThread();
            }
        };
        running = true;
        backgroundThread.start();
    }

    @Override
    public void stop() {
        running = false;
        clearWatch();
        if (backgroundThread != null) {
            notifyBackground();
            try {
                backgroundThread.join(1000);
            }
            catch (InterruptedException ex) {
                backgroundThread.interrupt();
            }
        }
    }

    @Override
    public void crash() {
        stop();
    }

    public void completeBackgroundWork() {
        do {
            notifyBackground();
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException ex) {
            }
        } while (!backgroundIdle);
    }
    
    protected void notifyBackground() {
        if (Thread.currentThread() != backgroundThread) {
            synchronized (backgroundThread) {
                backgroundThread.notifyAll();
            }
        }
    }
    
    private void backgroundThread() {
        backgroundIdle = false;
        try {
            while (running) {
                writeStatus();
                try {
                    synchronized(backgroundThread) {
                        backgroundIdle=true;
                        backgroundThread.wait(flushInterval);
                        backgroundIdle=false;
                    }
                } catch (InterruptedException ex) {
                    break;
                }
            }
        } catch (Exception ex) {
            logger.error("Error in metrics background thread", ex);
        }
    }
  

    protected Database getDatabase() {
        return fdbService.getDatabase();
    }

    // TODO: This should be used to remove the status when the service
    // shuts down, but can't currently be called in stop() due
    // to shutdown order. 
    private void clearStatus() {
        getDatabase()
        .run(new Function<Transaction,Void>() {
                 @Override
                 public Void apply(Transaction tr) {
                     tr.clear(instanceKey);
                     return null;
                 }
             });
    }
   
    private void writeStatus () {
        clearWatch();
        String status = generateStatus();
        final byte[] jsonData = Tuple2.from(status).pack();
        getDatabase()
        .run(new Function<Transaction,Void>() {
                 @Override
                 public Void apply(Transaction tr) {
                     tr.set (instanceKey, jsonData);
                     setWatch(tr);
                     return null;
                 }
             });
    }

    private void setWatch(Transaction tr) {
        // Initiate a watch (from this same transaction) for changes to the key
        // used to signal configuration changes.
        instanceWatch = tr.watch(instanceKey);
        instanceWatch.onReady(new Runnable() {
                @Override
                public void run() {
                    notifyBackground();
                }
            });
    }
    
    private void clearWatch() {
        if (instanceWatch != null) {
            instanceWatch.cancel();
            instanceWatch = null;
        }
    }

    private String generateStatus() {
        StringWriter str = new StringWriter();
        try {
            JsonGenerator gen = JsonUtils.createJsonGenerator(str);
            gen.writeStartObject();
            gen.writeStringField("name", "SQL Layer");
            gen.writeNumberField("timestamp", System.currentTimeMillis());
            gen.writeStringField("version", Main.VERSION_INFO.versionLong);
            gen.writeStringField("host", host);
            gen.writeStringField("port", port);
            summary(INSTANCE, INSTANCE_SQL, gen, false);
            summary(SERVERS, SERVERS_SQL, gen, true);
            summary(SESSIONS, SESSIONS_SQL, gen, true);
            summary(STATISTICS, STATISTICS_SQL, gen, false);
            summary(GARBAGE_COLLECTORS, GARBAGE_COLLECTORS_SQL, gen, true);
            summary(MEMORY_POOLS, MEMORY_POOLS_SQL, gen, true);
            gen.writeEndObject();
            gen.flush();
        } catch (JsonGenerationException ex) {
            logger.error("Unable to generate status due to JSON error: {}", ex);
            return null;
        } catch (IOException e) {
            logger.error ("Unable to generate status due to IOException: {}", e);
            return null;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("status: {}", str.toString());
        }
        return str.toString();
    }
    
    private static final String INSTANCE = "instance";
    private static final String INSTANCE_SQL =  "select server_store as store, server_jit_compiler_time as jit_compiler_time from information_schema.server_instance_summary";
    
    private static final String SERVERS = "servers";
    private static final String SERVERS_SQL = "select * from information_schema.server_servers";
    
    private static final String SESSIONS = "sessions";
    private static final String SESSIONS_SQL  = "select session_id, start_time, server_type, remote_address,"+ 
        "query_count, failed_query_count, query_from_cache, logged_statements," +
        "call_statement_count, ddl_statement_count, dml_statement_count, select_statement_count," + 
        "other_statement_count from information_schema.server_sessions";
    
    private static final String STATISTICS = "statistics";
    private static final String STATISTICS_SQL = "select * from information_schema.server_statistics_summary";
    
    private static final String GARBAGE_COLLECTORS = "garbage collectors";
    private static final String GARBAGE_COLLECTORS_SQL = "select * from information_schema.server_garbage_collectors";
    
    private static final String MEMORY_POOLS = "memory pools";
    private static final String MEMORY_POOLS_SQL = "select * from information_schema.server_memory_pools";
    
    protected void summary (String name, String sql, JsonGenerator gen, boolean arrayWrapper) throws JsonGenerationException, IOException {
       Properties props = new Properties();
       props.setProperty("database", TableName.INFORMATION_SCHEMA);
       try (Connection conn = jdbcService.getDriver().connect(JDBCDriver.URL, props);
               Statement s = conn.createStatement()) {

           if (arrayWrapper) {
               gen.writeArrayFieldStart(name);
           } else {
               gen.writeFieldName(name);
           }
           JDBCResultSet rs = (JDBCResultSet)s.executeQuery(sql);
           StringWriter strings = new StringWriter();
           PrintWriter writer = new PrintWriter(strings);
           collectResults(rs, writer, options);
           gen.writeRawValue(strings.toString());
           if (arrayWrapper) {
               gen.writeEndArray();
           }
       } catch (SQLException e) {
           logger.error("unable to generate summary for {}, inserting no data", name);
       }
    }

    private void collectResults(JDBCResultSet resultSet, PrintWriter writer, FormatOptions opt) throws SQLException {
        AkibanAppender appender = AkibanAppender.of(writer);

        SQLOutput cursor = new SQLOutput(resultSet);
        try {
            JsonRowWriter jsonRowWriter = new JsonRowWriter(cursor);
            if (jsonRowWriter.writeRowsFromOpenCursor(cursor, appender, "\n", cursor, opt)) {
                appender.append('\n');
            }
        } finally {
            cursor.close();
        }
    }

    private class SQLOutput extends GenericRowTracker implements RowCursor, JsonRowWriter.WriteRow {

        private JDBCResultSet resultSet;

        public SQLOutput (JDBCResultSet rs) {
            this.resultSet = rs;
        }
        @Override
        public void open() {
        }

        @Override
        public void close() {
        }

        @Override
        public Row next() {
            try {
                if (resultSet.next()) {
                    return resultSet.unwrap(Row.class);
                } else {
                    return null;
                }
            } catch(SQLException e) {
                throw new IllegalStateException(e);
            }
        }


        @Override
        public void jump(Row row, ColumnSelector columnSelector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isIdle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isActive() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isClosed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setIdle() {
        }

        @Override
        public String getRowName() {
            return null;
        }

        @Override
        public void write(Row row, AkibanAppender appender,
                FormatOptions options) {
            try {
                JDBCResultSetMetaData metaData = resultSet.getMetaData();
                boolean begun = false;
                for(int col = 1; col <= metaData.getColumnCount(); ++col) {
                    String colName = metaData.getColumnLabel(col);
                    ValueSource valueSource = row.value(col - 1);
                    JsonRowWriter.writeValue(colName, valueSource, appender, !begun, options);
                    begun = true;
                }
            } catch(SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
