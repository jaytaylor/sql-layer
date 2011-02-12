/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service.memcache;

import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessor;
import com.akiban.server.service.memcache.hprocessor.EmptyRows;
import com.akiban.server.service.memcache.hprocessor.Fetchrows;
import com.akiban.server.service.memcache.hprocessor.Scanrows;
import com.akiban.server.service.memcache.outputter.DummyOutputter;
import com.akiban.server.service.memcache.outputter.JsonOutputter;
import com.akiban.server.service.memcache.outputter.RawByteOutputter;
import com.akiban.server.service.memcache.outputter.RequestEchoOutputter;
import com.akiban.server.service.memcache.outputter.RowDataStringOutputter;

public interface MemcacheService extends HapiProcessor {
    void setHapiProcessor(WhichHapi processor);

    @SuppressWarnings("unused") // jmx
    enum WhichHapi {
        FETCHROWS(Fetchrows.instance()),
        EMPTY(EmptyRows.instance()),
        SCANROWS(null) {
            @Override
            public HapiProcessor getHapiProcessor() {
                return Scanrows.instance();
            }
        }
        ;

        private final HapiProcessor processor;

        WhichHapi(HapiProcessor processor) {
            this.processor = processor;
        }

        public HapiProcessor getHapiProcessor() {
            return processor;
        }
    }
    
    @SuppressWarnings("unused") // jmx
    enum OutputFormat {
        JSON(JsonOutputter.instance()),
        RAW(RawByteOutputter.instance()),
        DUMMY(DummyOutputter.instance()),
        PLAIN(RowDataStringOutputter.instance()),
        ECHO(RequestEchoOutputter.instance())
        ;
        private final HapiOutputter outputter;

        OutputFormat(HapiOutputter outputter) {
            this.outputter = outputter;
        }

        public HapiOutputter getOutputter() {
            return outputter;
        }
    }
}
