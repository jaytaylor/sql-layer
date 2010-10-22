package com.akiban.cserver.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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

    public Log logger()
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
            this.connection.new DDL(TEMPLATE_CREATE_PROGRESS_TABLE, schema).execute();
        }
    }

    private void recordEvent(String message)
    {
        if (connection != null) {
            long nowNsec = System.nanoTime();
            try {
                connection.new Update(TEMPLATE_LOG_PROGRESS, schema,
                                      ((double) (nowNsec - lastEventTimeNsec)) / ONE_BILLION,
                                      message).execute();
            } catch (SQLException e) {
                logger.error(String.format(
                        "Unable to record event in %s.progress: %s", schema,
                        message));
            }
            lastEventTimeNsec = nowNsec;
        }
    }

    private static final Log logger = LogFactory.getLog(Tracker.class);
    private static final int ONE_BILLION = 1000 * 1000 * 1000;
    private static final String TEMPLATE_CREATE_PROGRESS_TABLE = "create table if not exists %s.progress("
                                                                 + "    event_id int auto_increment, "
                                                                 + "    time timestamp not null, "
                                                                 + "    event_time_sec double not null, "
                                                                 + "    message varchar(500), " + "    primary key(event_id)" + ")";
    private static final String TEMPLATE_LOG_PROGRESS = "insert into %s.progress(time, event_time_sec, message) values (now(), %s, '%s')";
    private static final String TEMPLATE_RECENT_EVENTS = "select event_id, time, event_time_sec, message "
                                                         + "from %s.progress " + "where event_id > %s";

    private final String schema;
    private final DB.Connection connection;
    private long lastEventTimeNsec = System.nanoTime();
}
