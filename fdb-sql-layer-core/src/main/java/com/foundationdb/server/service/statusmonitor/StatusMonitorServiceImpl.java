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
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.sql.Main;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;
import com.foundationdb.util.JsonUtils;
import com.google.inject.Inject;

public class StatusMonitorServiceImpl implements StatusMonitorService, Service {

    private final ConfigurationService configService;
    private final FDBHolder fdbService;
    private final EmbeddedJDBCService jdbcService;
    private static final Logger logger = LoggerFactory.getLogger(StatusMonitorServiceImpl.class);

    public static final List<String> STATUS_MON_DIR = Arrays.asList("Status Monitor","Layers");
    public static final String STATUS_MON_LAYER_NAME = "SQL Layer";
    
    public static final String CONFIG_FLUSH_INTERVAL = "fdbsql.fdb.status.flush_interval";

    private byte[] instanceKey;
    private String host;
    private String port;
    protected Thread backgroundThread;
    protected volatile boolean running;
    private long flushInterval;
    private Future<Void> instanceWatch;
    
    @Inject
    public StatusMonitorServiceImpl (ConfigurationService configService, 
            FDBHolder fdbService,
            EmbeddedJDBCService jdbcService) {
        this.configService= configService;
        this.fdbService = fdbService;
        this.jdbcService = jdbcService;
    }
    
    @Override
    public void start() {
        flushInterval = Long.parseLong(configService.getProperty(CONFIG_FLUSH_INTERVAL));

        host = "127.0.0.1";
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        }
        catch (IOException ex) {
            // Ignore
        }
        port = configService.getProperty("fdbsql.postgres.port");
        String address = host + ":" + port;

        DirectorySubspace rootDirectory = new DirectoryLayer().createOrOpen(fdbService.getTransactionContext(), STATUS_MON_DIR).get();
        instanceKey = ByteArrayUtil.join(rootDirectory.pack(),
                Tuple2.from(STATUS_MON_LAYER_NAME, address).pack());

        logger.debug("InstanceKey {}", bytesToHexString(rootDirectory.pack()));
        backgroundThread = new Thread() {
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
        instanceWatch.cancel();
        notifyBackground();
        try {
            backgroundThread.join(1000);
        }
        catch (InterruptedException ex) {
            backgroundThread.interrupt();
        }
        clearStatus();
    }

    @Override
    public void crash() {
        stop();
    }
    
    protected void notifyBackground() {
        if (Thread.currentThread() != backgroundThread) {
            synchronized (backgroundThread) {
                backgroundThread.notifyAll();
            }
        }
    }
    
    private void backgroundThread() {
        try {
            while (running) {
                writeStatus();
                backgroundThread.wait(flushInterval);
            }
        } catch (Exception ex) {
            logger.error("Error in metrics background thread", ex);
        }
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

    protected Database getDatabase() {
        return fdbService.getDatabase();
    }

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
            gen.writeEndObject();
            gen.flush();
        } catch (JsonGenerationException ex) {
            return null;
        } catch (IOException e) {
            return null;
        }
        
        return str.toString();
    }
    
    public static String bytesToHexString(byte[] bytes) {  
        StringBuilder sb = new StringBuilder(bytes.length * 2);  
      
        Formatter formatter = new Formatter(sb);  
        for (byte b : bytes) {  
            formatter.format("%02x ", b);  
        }  
      
        return sb.toString();  
    }      
}
