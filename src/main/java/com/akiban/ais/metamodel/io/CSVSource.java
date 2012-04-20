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

package com.akiban.ais.metamodel.io;

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
