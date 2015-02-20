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

import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.subspace.Subspace;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.server.service.is.BasicInfoSchemaTablesService;
import com.foundationdb.server.service.is.ServerSchemaTablesService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager.BindingsConfigurationProvider;
import com.foundationdb.server.test.it.FDBITBase;
import com.foundationdb.tuple.Tuple2;
import com.foundationdb.util.JsonUtils;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.junit.Assert.*;

public class StatusMonitorServiceImplIT extends FDBITBase {

    /** For use by classes that cannot extend this class directly */
    public static BindingsConfigurationProvider doBind(BindingsConfigurationProvider provider) {
        return provider.bindAndRequire(StatusMonitorService.class, StatusMonitorServiceImpl.class)
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

    private void checkStatus(String statusJson) throws IOException {
        assertNotNull("No status json text", statusJson);

        JsonParser parser = JsonUtils.jsonParser(statusJson);

        assertEquals(JsonToken.START_OBJECT, parser.nextToken());
        assertEquals(JsonToken.VALUE_STRING, parser.nextValue());
        assertEquals("id", parser.getCurrentName());
        assertThat(parser.getText(), not(isEmptyOrNullString()));
        assertEquals(JsonToken.VALUE_STRING, parser.nextValue());
        assertEquals("name", parser.getCurrentName());
        assertEquals("SQL Layer", parser.getText());
        assertEquals(JsonToken.VALUE_NUMBER_INT, parser.nextValue());
        assertEquals("timestamp", parser.getCurrentName());
        assertEquals(JsonToken.VALUE_STRING, parser.nextValue());
        assertEquals("version", parser.getCurrentName());
        assertEquals(JsonToken.FIELD_NAME, parser.nextToken());

        assertEquals("instance", parser.getText());
        assertEquals(JsonToken.START_OBJECT, parser.nextToken());
        assertEquals(JsonToken.VALUE_STRING, parser.nextValue());
        assertEquals("id", parser.getCurrentName());
        assertThat(parser.getText(), not(isEmptyOrNullString()));
        assertEquals(JsonToken.VALUE_STRING, parser.nextValue());
        assertEquals("host", parser.getCurrentName());
        assertThat(parser.getText(), not(isEmptyOrNullString()));
        assertEquals(JsonToken.VALUE_STRING, parser.nextValue());
        assertEquals("store", parser.getCurrentName());
        assertThat(parser.getText(), not(isEmptyOrNullString()));
        assertEquals(JsonToken.VALUE_NUMBER_INT, parser.nextValue());
        assertEquals("jit_compiler_time", parser.getCurrentName());
        assertEquals(JsonToken.END_OBJECT, parser.nextToken());

        assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
        assertEquals("servers", parser.getText());
        assertEquals(JsonToken.START_ARRAY, parser.nextToken());

        // Check Servers array, which should have one object, the internal JDCB connection service.

        assertEquals(JsonToken.START_OBJECT, parser.nextToken());
        assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
        assertEquals("server_type", parser.getText());
        assertEquals(JsonToken.VALUE_STRING, parser.nextToken());
        assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
        assertEquals("local_port", parser.getText());

        JsonToken port = parser.nextToken();
        assertTrue (port == JsonToken.VALUE_NUMBER_INT || port == JsonToken.VALUE_NULL);
        assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
        assertEquals("start_time", parser.getText());
        assertEquals(JsonToken.VALUE_NUMBER_INT, parser.nextToken());
        assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
        assertEquals("session_count", parser.getText());
        assertEquals(JsonToken.VALUE_NUMBER_INT, parser.nextToken());
        assertEquals(JsonToken.END_OBJECT, parser.nextToken());
        assertEquals(JsonToken.END_ARRAY, parser.nextToken());

        assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
        assertEquals("sessions", parser.getText());
        assertEquals(JsonToken.START_ARRAY, parser.nextToken());
        // There should be no sessions
        assertEquals(JsonToken.END_ARRAY, parser.nextToken());

        assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
        assertEquals("statistics", parser.getText());
        assertEquals(JsonToken.START_OBJECT, parser.nextToken());
        parser.skipChildren();
        assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
        assertEquals("garbage_collectors", parser.getText());
        assertEquals(JsonToken.START_ARRAY, parser.nextToken());
        parser.skipChildren();
        assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
        assertEquals("memory_pools", parser.getText());
        assertEquals(JsonToken.START_ARRAY, parser.nextToken());
        parser.skipChildren();
        assertEquals(JsonToken.END_OBJECT, parser.nextToken());
    }

    private String readStatusJson() {
        return fdbHolder().getTransactionContext().run(
            new Function<Transaction,String> () {
                @Override
                public String apply(Transaction tr) {
                    byte[] status = tr.get(statusMonitorService().instanceKey).get();
                    if (status == null) return null;
                    return Tuple2.fromBytes(status).getString(0);
                }
            });
    }

    private void waitAndCheckStatus() throws InterruptedException, IOException {
        // Wait up to 5s for watch to fire and status to be written
        String json = null;
        for(int i = 0; (json == null) && (i < 50); ++i) {
            Thread.sleep(100);
            json = readStatusJson();
        }
        checkStatus(json);
    }

    @Test
    public void verifyStartupStatus() throws IOException {
        String json = readStatusJson();
        checkStatus(json);
    }

    @Test
    public void verifyStatusAfterClearKey() throws IOException, InterruptedException {
        // Clear just the key
        fdbHolder().getTransactionContext().run(
            new Function<Transaction,Void> () {
                @Override
                public Void apply(Transaction tr) {
                    tr.clear(statusMonitorService().instanceKey);
                    return null;
                }
            });
        waitAndCheckStatus();
    }

    @Test
    public void verifyStatusAfterClearRange() throws IOException, InterruptedException {
        // Clear entire layer directory like real Status Monitor
        fdbHolder().getTransactionContext().run(
            new Function<Transaction,Void> () {
                @Override
                public Void apply(Transaction tr) {
                    Subspace smDir = DirectoryLayer.getDefault().open(tr, StatusMonitorServiceImpl.STATUS_MONITOR_DIR).get();
                    tr.clear(smDir.range());
                    return null;
                }
            });
        waitAndCheckStatus();
    }
}
