/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
