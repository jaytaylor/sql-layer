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
import com.akiban.rest.resources.DefaultResource;
import com.akiban.rest.resources.EntityResource;
import com.akiban.rest.resources.ModelResource;
import com.akiban.rest.resources.ProcedureCallResource;
import com.akiban.rest.resources.SQLResource;
import com.akiban.rest.resources.SecurityResource;
import com.akiban.rest.resources.VersionResource;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.restdml.RestDMLService;
import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.google.inject.Inject;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.Arrays;

public class RestServiceImpl implements RestService, Service {
    private final ConfigurationService configService;
	private final HttpConductor http;
    // Used by various resources
    private final RestDMLService restDMLService;
    private final SessionService sessionService;
    private final TransactionService transactionService;
    private final SecurityService securityService;
    private final DXLService dxlService;

	private volatile ServletContextHandler handler;
	

	@Inject
    public RestServiceImpl(ConfigurationService configService,
                           HttpConductor http,
                           RestDMLService restDMLService,
                           SessionService sessionService,
                           TransactionService transactionService,
                           SecurityService securityService,
                           DXLService dxlService) {
        this.configService = configService;
		this.http = http;
        this.restDMLService = restDMLService;
        this.sessionService = sessionService;
        this.transactionService = transactionService;
        this.securityService = securityService;
        this.dxlService = dxlService;
    }

    @Override
    public String getContextPath() {
        return configService.getProperty("akserver.rest.context_path");
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
        handler = new ServletContextHandler();
        handler.setContextPath(getContextPath());
        handler.addServlet(new ServletHolder(new ServletContainer(createResourceConfigV1())), "/*");
        http.registerHandler(handler);
	}

    private ResourceConfig createResourceConfigV1() {
        DefaultResourceConfig config = new DefaultResourceConfig();
        ResourceRequirements reqs = new ResourceRequirements(
                dxlService, restDMLService, securityService, sessionService, transactionService, this
        );
        config.getSingletons().addAll(Arrays.asList(
                new EntityResource(reqs),
                new ModelResource(reqs),
                new ProcedureCallResource(reqs),
                new SecurityResource(reqs),
                new SQLResource(reqs),
                new VersionResource(reqs),
                // This must be last to capture anything not handled above 
                new DefaultResource(reqs)
        ));
        return config;
    }
}
