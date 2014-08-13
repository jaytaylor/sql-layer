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

package com.foundationdb.http;

import com.foundationdb.rest.RestService;
import com.foundationdb.rest.RestServiceImpl;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HttpThreadedLoginIT extends ITBase
{
    private static final Logger LOG = LoggerFactory.getLogger(HttpThreadedLoginIT.class);

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .bindAndRequire(RestService.class, RestServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fdbsql.http.login", "basic");
        return properties;
    }

    private static int openRestURL(String userInfo, int port, String path) throws Exception {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        URI uri = new URI("http", userInfo, "localhost", port, path, null, null);
        HttpGet get = new HttpGet(uri);
        HttpResponse response = client.execute(get);
        int code = response.getStatusLine().getStatusCode();
        EntityUtils.consume(response.getEntity());
        client.close();
        return code;
    }

    @Test
    public void oneThread() throws InterruptedException {
        run(1);
    }

    @Test
    public void fiveThreads() throws InterruptedException {
        run(5);
    }

    @Test
    public void tenThreads() throws InterruptedException {
        run(10);
    }

    @Test
    public void twentyThreads() throws InterruptedException {
        run(20);
    }


    private void run(int count) throws InterruptedException {
        final int port = serviceManager().getServiceByClass(HttpConductor.class).getPort();
        final String context = serviceManager().getServiceByClass(RestService.class).getContextPath();
        final String path = context + "/version";
        final UncaughtHandler uncaughtHandler = new UncaughtHandler();

        Thread threads[] = new Thread[count];
        for(int i = 0; i < count; ++i) {
            threads[i] = new Thread(new TestRunnable(port, path, i), "Thread"+i);
            threads[i].setUncaughtExceptionHandler(uncaughtHandler);
        }
        for(int i = 0; i < count; ++i) {
            threads[i].start();
        }
        for(int i = 0; i < count; ++i) {
            threads[i].join();
        }

        for(Throwable entry : uncaughtHandler.uncaught.values()) {
            LOG.error("Uncaught exception", entry);
        }
        assertEquals("uncaught exception count", 0, uncaughtHandler.uncaught.size());
    }

    private static class UncaughtHandler implements Thread.UncaughtExceptionHandler {
        public final Map<Thread,Throwable> uncaught = Collections.synchronizedMap(new HashMap<Thread,Throwable>());

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            uncaught.put(t, e);
        }
    }

    private static class TestRunnable implements Runnable {
        private final int port;
        private final String url;
        private final int userNum;

        public TestRunnable(int port, String url, int userNum) {
            this.port = port;
            this.url = url;
            this.userNum = userNum;
        }

        @Override
        public void run() {
            String userInfo = String.format("user_%d:password", userNum);
            try {
                assertEquals(userInfo, HttpStatus.SC_UNAUTHORIZED, openRestURL(userInfo, port, url));
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
