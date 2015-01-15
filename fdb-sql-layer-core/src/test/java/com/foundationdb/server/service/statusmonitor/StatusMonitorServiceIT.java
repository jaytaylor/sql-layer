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

import org.junit.Test;

import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.server.service.metrics.FDBMetricsService;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager.BindingsConfigurationProvider;
import com.foundationdb.server.test.it.FDBITBase;
import com.foundationdb.tuple.Tuple2;

import static org.junit.Assert.*;

public class StatusMonitorServiceIT extends FDBITBase {

    /** For use by classes that cannot extend this class directly */
    public static BindingsConfigurationProvider doBind(BindingsConfigurationProvider provider) {
        return provider.bind(StatusMonitorService.class, StatusMonitorServiceImpl.class);
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
    }


}
