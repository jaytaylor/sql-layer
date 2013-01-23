/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.akiban.rest;

import com.akiban.http.HttpConductor;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.google.inject.Inject;
import com.google.inject.servlet.GuiceFilter;
import org.eclipse.jetty.servlet.ServletContextHandler;

import javax.servlet.DispatcherType;
import java.util.EnumSet;

public class RestServiceImpl implements RestService, Service {
    private final ConfigurationService configService;
	private final HttpConductor http;
	private volatile ServletContextHandler handler;

	@Inject
	public RestServiceImpl(ConfigurationService configService, HttpConductor http) {
        this.configService = configService;
		this.http = http;
	}

	@Override
	public void start() {
		registerConnector(http);
	}

	@Override
	public void stop() {
        http.unregisterHandler(handler);
        this.handler = null;
	}

	@Override
	public void crash() {
		stop();
	}

	private void registerConnector(HttpConductor http) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath(configService.getProperty("akserver.rest.context_path"));
        context.addFilter(GuiceFilter.class, "/*", EnumSet.<DispatcherType> of(DispatcherType.REQUEST));
        context.addServlet(EmptyServlet.class, "/*");

        this.handler = context;
		http.registerHandler(this.handler);
	}
}
