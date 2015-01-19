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

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.server.service.is.BasicInfoSchemaTablesService;
import com.foundationdb.server.service.is.ServerSchemaTablesService;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager.BindingsConfigurationProvider;
import com.foundationdb.server.test.it.FDBITBase;
import com.foundationdb.tuple.Tuple2;
import com.foundationdb.util.JsonUtils;

import static org.junit.Assert.*;

public class StatusMonitorServiceIT extends FDBITBase {

    /** For use by classes that cannot extend this class directly */
    public static BindingsConfigurationProvider doBind(BindingsConfigurationProvider provider) {
        return provider.require(StatusMonitorService.class)
                .require(BasicInfoSchemaTablesService.class)
                .require(ServerSchemaTablesService.class);
    }

    @Override
    protected BindingsConfigurationProvider serviceBindingsProvider() {
        return doBind(super.serviceBindingsProvider());
    }
    
    protected StatusMonitorServiceImpl statusMonitorService() {
        return (StatusMonitorServiceImpl)serviceManager().getServiceByClass(StatusMonitorService.class);
    }

    @Test
    public void verifyStartupStatus() {
        statusMonitorService().completeBackgroundWork();
        String results = 
        this.fdbHolder().getTransactionContext()
        .run(new Function<Transaction,String> () {
                 @Override
                 public String apply(Transaction tr) {
                     byte[] status = tr.get(statusMonitorService().instanceKey).get();
                     if (status == null) return null;
                     String results = Tuple2.fromBytes(status).getString(0);
                     return results;
                 }
             }); 
        assertNotNull (results);
        
        try {
            JsonParser parser = JsonUtils.jsonParser(results);
            
            assertEquals(JsonToken.START_OBJECT, parser.nextToken());
            assertEquals(JsonToken.VALUE_STRING, parser.nextValue());
            assertEquals("name", parser.getCurrentName());
            assertEquals("SQL Layer", parser.getText());
            assertEquals(JsonToken.VALUE_NUMBER_INT, parser.nextValue());
            assertEquals("timestamp", parser.getCurrentName());
            assertEquals(JsonToken.VALUE_STRING, parser.nextValue());
            assertEquals("version", parser.getCurrentName());
            assertEquals(JsonToken.VALUE_STRING, parser.nextValue());
            assertEquals("host", parser.getCurrentName());
            assertEquals(JsonToken.VALUE_STRING, parser.nextValue());
            assertEquals("port", parser.getCurrentName());
            assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
            assertEquals("instance", parser.getText());
            assertEquals(JsonToken.START_OBJECT, parser.nextToken());
            parser.skipChildren();
            assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
            assertEquals("servers", parser.getText());
            assertEquals(JsonToken.START_ARRAY, parser.nextToken());
            parser.skipChildren();
            assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
            assertEquals("sessions", parser.getText());
            assertEquals(JsonToken.START_ARRAY, parser.nextToken());
            parser.skipChildren();
            assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
            assertEquals("statistics", parser.getText());
            assertEquals(JsonToken.START_OBJECT, parser.nextToken());
            parser.skipChildren();
            assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
            assertEquals("garbage collectors", parser.getText());
            assertEquals(JsonToken.START_ARRAY, parser.nextToken());
            parser.skipChildren();
            assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
            assertEquals("memory pools", parser.getText());
            assertEquals(JsonToken.START_ARRAY, parser.nextToken());
            parser.skipChildren();
            assertEquals(JsonToken.END_OBJECT, parser.nextToken());
        } catch (IOException e) {
            assertTrue("IOException", false);
        }
        
    }


}
