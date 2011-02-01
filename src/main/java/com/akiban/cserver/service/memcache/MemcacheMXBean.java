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

package com.akiban.cserver.service.memcache;

import com.akiban.cserver.api.HapiProcessor;
import com.akiban.cserver.service.memcache.hprocessor.EmptyRows;
import com.akiban.cserver.service.memcache.hprocessor.Fetchrows;
import com.akiban.cserver.service.memcache.outputter.DummyOutputter;
import com.akiban.cserver.service.memcache.outputter.JsonOutputter;
import com.akiban.cserver.service.memcache.outputter.RawByteOutputter;
import com.akiban.cserver.service.memcache.outputter.RowDataStringOutputter;

@SuppressWarnings("unused") // these are queried/set via JMX
public interface MemcacheMXBean {

    OutputFormat getOutputFormat();
    void setOutputFormat(OutputFormat whichFormat);
    OutputFormat[] getAvailableOutputFormats();

    WhichHapi getHapiProcessor();
    void setHapiProcessor(WhichHapi whichProcessor);
    WhichHapi[] getAvailableHapiProcessors();

    enum WhichHapi {
        FETCHROWS(Fetchrows.instance()),
        EMPTY(EmptyRows.instance())
        ;

        private final HapiProcessor processor;

        WhichHapi(HapiProcessor processor) {
            this.processor = processor;
        }

        public HapiProcessor getHapiProcessor() {
            return processor;
        }
    }

    enum OutputFormat {
        JSON(JsonOutputter.instance()),
        RAW(RawByteOutputter.instance()),
        DUMMY(DummyOutputter.instance()),
        PLAIN(RowDataStringOutputter.instance())
        ;
        private final HapiProcessor.Outputter outputter;

        OutputFormat(HapiProcessor.Outputter outputter) {
            this.outputter = outputter;
        }

        public HapiProcessor.Outputter getOutputter() {
            return outputter;
        }
    }
}
