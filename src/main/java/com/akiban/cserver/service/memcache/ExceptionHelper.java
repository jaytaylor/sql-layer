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

import com.akiban.cserver.api.HapiRequestException;
import com.akiban.cserver.api.HapiRequestException.ReasonCode;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.thimbleware.jmemcached.protocol.text.MemcachedPipelineFactory.USASCII;

final class ExceptionHelper {
    private static final ChannelBuffer DEFAULT_ERROR = ChannelBuffers.copiedBuffer("SERVER_ERROR\r\n", USASCII);
    private static final Map<ReasonCode,ChannelBuffer> BUFFERS_MAP = getChannelsMap();

    private static Map<ReasonCode, ChannelBuffer> getChannelsMap() {
        Map<ReasonCode, ChannelBuffer> map = new HashMap<ReasonCode, ChannelBuffer>();
        for (ReasonCode reasonCode : ReasonCode.values()) {
            String err = String.format("SERVER_ERROR %s\r\n", reasonCode.name());
            map.put(reasonCode, ChannelBuffers.copiedBuffer(err, USASCII));
        }
        return Collections.unmodifiableMap(map);
    }

    static ChannelBuffer forException(Throwable t) {
        ChannelBuffer buff = null;
        if (t instanceof HapiRequestException) {
            HapiRequestException hre = (HapiRequestException)t;
            buff = BUFFERS_MAP.get(hre.getReasonCode());
        }
        if (buff == null) {
            buff = DEFAULT_ERROR;
        }
        return buff.duplicate();
    }
}
