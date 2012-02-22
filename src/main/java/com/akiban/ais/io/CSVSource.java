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

package com.akiban.ais.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.metamodel.Source;
import com.akiban.server.error.AisCSVErrorException;

public class CSVSource extends Source
{
    @Override
    public void close()
    {
        try {
        input.close();
        } catch (IOException ex) {
            LOG.error("IOException while closing CSV-formatted AIS source", ex);
            throw new AisCSVErrorException ("CSVSource close", ex.getMessage());
        }
    }

    // PersistitSource interface

    public CSVSource(BufferedReader input)
    {
        this.input = input;
        advance();
    }

    @Override 
    public int readVersion()
    {
        int modelVersion = Integer.parseInt(unquote(row[field]));
        advance();
        return modelVersion;
    }
    
    @Override
    protected final void read(String typename, Receiver receiver)
    {
        ModelObject modelObject = MetaModel.only().definition(typename);
        while (typeIs(typename)) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (ModelObject.Attribute attribute : modelObject.attributes()) {
                Object value = null;
                switch (attribute.type()) {
                    case INTEGER:
                        value = readIntOrNull();
                        break;
                    case LONG:
                        value = readLongOrNull();
                        break;
                    case BOOLEAN:
                        value = readBooleanOrNull();
                        break;
                    case STRING:
                        value = readStringOrNull();
                        break;
                    default:
                        assert false;
                }
                map.put(attribute.name(), value);
            }
            receiver.receive(map);
            advance();
        }
    }

    private void advance()
    {
        try {
            String line = input.readLine();
            if (line == null) {
                row = null;
            } else {
                row = line.split(",");
                for (int i = 0; i < row.length; i++) {
                    row[i] = unquote(row[i].trim());
                }
                field = 0;
            }
        } catch (IOException e) {
            LOG.error("IOException while reading CSV-formatted AIS source", e);
            throw new AisCSVErrorException ("CSVSource advance", e.getMessage());
        }
    }

    private boolean typeIs(String type)
    {
        return row != null && row[0].equals(type);
    }

    private String field()
    {
        return unquote(row[++field]);
    }

    private String unquote(String s)
    {
        if (s.length() > 0 && s.charAt(0) == QUOTE && s.charAt(s.length() - 1) == QUOTE) {
            s = s.substring(1, s.length() - 1);
        }
        return s;
    }

    private String readStringOrNull()
    {
        String field = field();
        return field.length() == 0 ? null : field;
    }

    private Integer readIntOrNull()
    {
        String field = field();
        return field.length() == 0 ? null : Integer.parseInt(field);
    }

    private Long readLongOrNull()
    {
        String field = field();
        return field.length() == 0 ? null : Long.parseLong(field);
    }

    private Boolean readBooleanOrNull()
    {
        String field = field();
        return field.equals("-1") ? null : !field.equals("0");
    }

    // State

    private static final Logger LOG = LoggerFactory.getLogger(CSVSource.class.getName());
    private static final char QUOTE = '"';

    private final BufferedReader input;
    private String[] row;
    private int field;
}
