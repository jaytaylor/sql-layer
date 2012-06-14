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

package com.akiban.server.loader;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tracker
{
    public void info(String template, Object... args)
    {
        String message = String.format(template, args);
        logger.info(message);
        recordEvent(message);
    }

    public void error(String template, Exception e, Object... args)
    {
        String message = String.format(template, args);
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        String stack = stringWriter.toString();
        logger.error(message, e);
        logger.error(stack);
        recordEvent(message);
        recordEvent(stack);
    }

    public Logger logger()
    {
        return logger;
    }

    public List<Event> recentEvents(int lastEventId) throws Exception
    {
        final List<Event> events = new ArrayList<Event>();
        if (connection != null) {
            connection.new Query(TEMPLATE_RECENT_EVENTS, schema, lastEventId)
            {
                @Override
                protected void handleRow(ResultSet resultSet) throws Exception
                {
                    int c = 0;
                    int eventId = resultSet.getInt(++c);
                    Date timestamp = resultSet.getDate(++c);
                    double timeSec = resultSet.getDouble(++c);
                    String message = resultSet.getString(++c);
                    events.add(new Event(eventId, timestamp, timeSec, message));
                }
            }.execute();
        }
        return events;
    }

    public Tracker(DB db, String schema) throws SQLException
    {
        this.connection = db == null ? null : db.new Connection();
        this.schema = schema;
        if (this.connection != null) {
            this.connection.new DDL(TEMPLATE_CREATE_PROGRESS_TABLE, schema, MAX_MESSAGE_SIZE).execute();
        }
    }

    private void recordEvent(String message)
    {
        if (connection != null) {
            long nowNsec = System.nanoTime();
            try {
                if (message.length() > MAX_MESSAGE_SIZE) {
                    message = message.substring(0, MAX_MESSAGE_SIZE);
                }
                connection.new Update(TEMPLATE_LOG_PROGRESS, schema,
                                      ((double) (nowNsec - lastEventTimeNsec)) / ONE_BILLION,
                                      message).execute();
            } catch (SQLException e) {
                logger.error(String.format(
                        "Unable to record event in %s.progress: %s", schema,
                        e.getMessage()));
            }
            lastEventTimeNsec = nowNsec;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(Tracker.class);
    private static final int ONE_BILLION = 1000 * 1000 * 1000;
    private static final int MAX_MESSAGE_SIZE = 5000;
    private static final String TEMPLATE_CREATE_PROGRESS_TABLE = "create table if not exists %s.progress("
                                                                 + "    event_id int auto_increment, "
                                                                 + "    time timestamp not null, "
                                                                 + "    event_time_sec double not null, "
                                                                 + "    message varchar(%s), "
                                                                 + "    primary key(event_id)" + ")"
                                                                 + "    engine = myisam";
    private static final String TEMPLATE_LOG_PROGRESS = "insert into %s.progress(time, event_time_sec, message) values (now(), %s, '%s')";
    private static final String TEMPLATE_RECENT_EVENTS = "select event_id, time, event_time_sec, message "
                                                         + "from %s.progress " + "where event_id > %s";

    private final String schema;
    private final DB.Connection connection;
    private long lastEventTimeNsec = System.nanoTime();
}
