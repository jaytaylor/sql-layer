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

import com.akiban.rest.resources.DataAccessOperationsResource;
import com.akiban.rest.resources.FaviconResource;
import com.akiban.rest.resources.SqlExecutionResource;
import com.akiban.rest.resources.SqlQueryResource;
import com.akiban.rest.resources.VersionResource;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Guice Module which registers essential classes.
 */
public class RestServiceModule extends ServletModule {
    // Hang onto reference or setting will get GC-ed.
    private Logger guiceFilterLogger = null;

    @Override
    protected void configureServlets() {
        // GuiceFilter has a static member that causes a superfluous (for us) warning when services
        // (the injector, really) are cycled during the ITs. Disable it.
        guiceFilterLogger = Logger.getLogger(GuiceFilter.class.getName());
        guiceFilterLogger.setLevel(Level.OFF);

        bind(FaviconResource.class).asEagerSingleton();
        bind(DataAccessOperationsResource.class).asEagerSingleton();
        bind(SqlQueryResource.class).asEagerSingleton();
        bind(SqlExecutionResource.class).asEagerSingleton();
        bind(VersionResource.class).asEagerSingleton();

        bind(ConnectionCloseFilter.class).asEagerSingleton();

        serve("*").with(GuiceContainer.class);
        filter("*").through(ConnectionCloseFilter.class);
    }
}
