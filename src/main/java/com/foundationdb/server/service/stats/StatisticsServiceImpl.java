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

package com.foundationdb.server.service.stats;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.SecurityServiceImpl.Routines;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;
import com.google.inject.Inject;

public final class StatisticsServiceImpl implements StatisticsService, Service {

    private static final String STATISTICS_PROPERTY = "fdbsql.statistics";
    private static final String SCHEMA = TableName.SYS_SCHEMA;
    private static final String SET_ENABLED = "taps_set_enabled";
    private static final String RESET 		= "taps_reset";

    @Inject
    public StatisticsServiceImpl(ConfigurationService config, 
            SchemaManager schemaManager) {
        this.config = config;
        this.schemaManager = schemaManager;
    }

    @Override
    public void setEnabled(final String regExPattern, final boolean on) {
        Tap.setEnabled(regExPattern, on);
    }

    @Override
    public void reset(final String regExPattern) {
        Tap.reset(regExPattern);
    }

    @Override
    public TapReport[] getReport(final String regExPattern) {
        return Tap.getReport(regExPattern);
    }


    @Override
    public void start() {
        String stats_enable = config.getProperty(STATISTICS_PROPERTY);

        if (stats_enable.length() > 0) {
            Tap.setEnabled(stats_enable, true);
        }
        registerSystemObjects();
    }

    @Override
    public void stop() {
    	deregisterSystemObjects();
    }
    
    
    @Override
    public void crash() {
    	deregisterSystemObjects();
    }

    protected void registerSystemObjects() {
        AkibanInformationSchema ais = buildSystemObjects();
        schemaManager.registerSystemRoutine(ais.getRoutine(SCHEMA, SET_ENABLED));
        schemaManager.registerSystemRoutine(ais.getRoutine(SCHEMA, RESET));
    }

    protected void deregisterSystemObjects() {
        schemaManager.unRegisterSystemRoutine(new TableName(SCHEMA, SET_ENABLED));
        schemaManager.unRegisterSystemRoutine(new TableName(SCHEMA, RESET));
    	
    }
    
    protected AkibanInformationSchema buildSystemObjects() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, schemaManager.getTypesTranslator());
        
        builder.procedure(SET_ENABLED)
        .language("java", Routine.CallingConvention.JAVA)
        .paramStringIn("tap_names", 128)
        .paramStringIn("enable", 128)
        .externalName(Routines.class.getName(), "setEnabled");
        
        builder.procedure(RESET)
        .language("java", Routine.CallingConvention.JAVA)
        .paramStringIn("tap_names", 128)
        .externalName(Routines.class.getName(), "resetTaps");
        
        return builder.ais(true);
    }
    
    private final ConfigurationService config;
    private final SchemaManager schemaManager;

    // TODO: Temporary way of accessing these via stored procedures.
    public static class Routines {
        public static void setEnabled(String tapNames, String enable) {
             boolean enabled = Boolean.parseBoolean(enable);
             getService().setEnabled(tapNames, enabled);
        }

        public static void resetTaps(String tapNames) {
        	getService().reset(tapNames);
        }
        
        private static StatisticsService getService() {
            ServerQueryContext context = ServerCallContextStack.getCallingContext();
            return context.getServiceManager().getStatisticsService();
        }
    }
}

