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

package com.akiban.server;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionDispatch;
import com.akiban.server.types.ConversionSource;
import com.persistit.Key;

public final class KeyConversionTarget {

    // KeyConversionTarget interface

    public void attach(Key key) {
        this.key = key;
    }

    public void append(ConversionSource conversionSource) {
        if (conversionSource.isNull()) {
            key.append(null);
        } else {
            KeyAppender appender = dispatch.get(conversionSource.conversionType());
            appender.appendSource(conversionSource, key);
        }
    }

    // object interface

    @Override
    public String toString() {
        return key.toString();
    }

    // object state

    private Key key;
    private final ConversionDispatch<KeyAppender> dispatch = new ConversionDispatch<KeyAppender>(
            new LongAppender(),
            new LongAppender(),
            new StringAppender()
    );

    // nested classes

    private interface KeyAppender {
        void appendSource(ConversionSource source, Key key);
    }

    private static class LongAppender implements KeyAppender {
        @Override
        public void appendSource(ConversionSource source, Key key) {
            assert source.conversionType() == AkType.LONG : source.conversionType();
            key.append(source.getLong());
        }
    }

    private static class StringAppender implements KeyAppender {
        @Override
        public void appendSource(ConversionSource source, Key key) {
            assert source.conversionType() == AkType.STRING : source.conversionType();
            key.append(source.getObject(String.class));
        }
    }
}
