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
import com.akiban.cserver.service.memcache.outputter.DummyByteOutputter;
import com.akiban.cserver.service.memcache.outputter.JsonOutputter;
import com.akiban.cserver.service.memcache.outputter.RawByteOutputter;
import com.akiban.cserver.service.memcache.outputter.RowDataStringOutputter;

public interface MemcacheMXBean {
    public OutputFormat getOutputFormat();
    public void setOutputFormat(OutputFormat whichFormat);
    public String[] getAvailableOutputFormats();

    @SuppressWarnings("unused") // these are queried/set via JMX
    enum OutputFormat {
        JSON(JsonOutputter.instance()),
        RAW(RawByteOutputter.instance()),
        DUMMY(DummyByteOutputter.instance()),
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
