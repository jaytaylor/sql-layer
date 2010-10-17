package com.akiban.ais.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.Source;

public class CSVSource extends Source
{


    @Override
    public void close() throws Exception
    {
        input.close();
    }

    // PersistitSource interface

    public CSVSource(BufferedReader input)
    {
        this.input = input;
        advance();
    }

    @Override
    protected final void read(String typename, Receiver receiver) throws Exception
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
            row = null;
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

    private static final Log LOG = LogFactory.getLog(CSVSource.class.getName());
    private static final char QUOTE = '"';

    private final BufferedReader input;
    private String[] row;
    private int field;
}