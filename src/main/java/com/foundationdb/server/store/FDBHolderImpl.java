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
package com.foundationdb.server.store;

import com.foundationdb.NetworkOptions;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.server.error.ClusterFileNotReadableException;
import com.foundationdb.server.error.ClusterFileTooLargeException;
import com.foundationdb.server.error.NoClusterFileException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.FDBException;
import com.foundationdb.util.ArgumentValidation;
import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FDBHolderImpl implements FDBHolder, Service {
    private static final Logger LOG = LoggerFactory.getLogger(FDBHolderImpl.class.getName());

    private static final String CONFIG_API_VERSION = "fdbsql.fdb.api_version";
    private static final String CONFIG_CLUSTER_FILE = "fdbsql.fdb.cluster_file";
    private static final String CONFIG_TRACE_DIRECTORY = "fdbsql.fdb.trace_directory";
    private static final String CONFIG_ROOT_DIR = "fdbsql.fdb.root_directory";
    private static final String CONFIG_TLS_PLUGIN = "fdbsql.fdb.tls.plugin";
    private static final String CONFIG_TLS_CERT_PATH = "fdbsql.fdb.tls.cert_path";
    private static final String CONFIG_TLS_KEY_PATH = "fdbsql.fdb.tls.key_path";
    private static final String CONFIG_TLS_VERIFY_PEERS = "fdbsql.fdb.tls.verify_peers";
    private static final String CONFIG_KNOB_PREFIX = "fdbsql.fdb.knobs.";

    private final ConfigurationService configService;

    private int apiVersion;
    private FDB fdb;
    private Database db;
    private DirectorySubspace rootDirectory;

    @Inject
    public FDBHolderImpl(ConfigurationService configService) {
        this.configService = configService;
    }


    //
    // Service
    //

    @Override
    public void start() {
        // Just one FDB for whole JVM and its dispose doesn't do anything.
        if(fdb == null) {
            apiVersion = Integer.parseInt(configService.getProperty(CONFIG_API_VERSION));
            LOG.info("Staring with API Version {}", apiVersion);
            fdb = FDB.selectAPIVersion(apiVersion);
            // Legal to call more than once but will only apply the first time (network started once)
            setOptions(fdb.options());
        }
        String clusterFile = configService.getProperty(CONFIG_CLUSTER_FILE);
        boolean isDefault = clusterFile.isEmpty();
        clusterFile = isDefault ? "DEFAULT" : clusterFile;
        LOG.info("Opening cluster file {}", clusterFile);
        try {
            db = isDefault ? fdb.open() : fdb.open(clusterFile);
        } catch (FDBException e) {
            if (e.getCode() == 1515) { // no_cluster_file_found
                throw new NoClusterFileException(clusterFile);
            }
            else if (e.getCode() == 1513) { // file_not_readable
                throw new ClusterFileNotReadableException(clusterFile);
            }
            else if (e.getCode() == 1516) { // cluster_file_too_large
                throw new ClusterFileTooLargeException(clusterFile);
            }
            throw e;
        }
        String rootDirName = configService.getProperty(CONFIG_ROOT_DIR);
        List<String> rootDirPath = parseDirString(rootDirName);
        rootDirectory = new DirectoryLayer().createOrOpen(db, rootDirPath).get();
    }

    @Override
    public void stop() {
        if(db != null)
            db.dispose();
        db = null;
    }

    @Override
    public void crash() {
        stop();
    }


    //
    // FDBHolder
    //

    @Override
    public int getAPIVersion() {
        return apiVersion;
    }

    @Override
    public FDB getFDB() {
        return fdb;
    }

    @Override
    public Database getDatabase() {
        return db;
    }

    @Override
    public DirectorySubspace getRootDirectory() {
        return rootDirectory;
    }


    //
    // Internal
    //

    private void setOptions(NetworkOptions options) {
        String val = configService.getProperty(CONFIG_TRACE_DIRECTORY);
        if (!val.isEmpty()) {
            options.setTraceEnable(val);
        }
        val = configService.getProperty(CONFIG_TLS_PLUGIN);
        if (!val.isEmpty()) {
            options.setTLSPlugin(val);
        }
        val = configService.getProperty(CONFIG_TLS_CERT_PATH);
        if (!val.isEmpty()) {
            options.setTLSCertPath(val);
        }
        val = configService.getProperty(CONFIG_TLS_KEY_PATH);
        if (!val.isEmpty()) {
            options.setTLSKeyPath(val);
        }
        val = configService.getProperty(CONFIG_TLS_VERIFY_PEERS);
        if (!val.isEmpty()) {
            byte[] bytes = val.getBytes(Charset.forName("UTF8"));
            options.setTLSVerifyPeers(bytes);
        }
        Properties knobs = configService.deriveProperties(CONFIG_KNOB_PREFIX);
        for(String name : knobs.stringPropertyNames()) {
            val = knobs.getProperty(name);
            options.setKnob(String.format("%s=%s", name, val));
        }
    }

    static List<String> parseDirString(String dirString) {
        ArgumentValidation.notNull("dirString", dirString);
        // Excess whitespace, ends with /, back to forward and deduplicate
        String normalized = (dirString.trim() + "/").replace("\\", "/").replace("//", "/");
        String[] parts = normalized.split("/");
        return Arrays.asList(parts);
    }
}
