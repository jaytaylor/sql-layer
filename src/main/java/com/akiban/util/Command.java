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

package com.akiban.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Command
{
    public static Command saveOutput(String ... commandLine)
    {
        return new SaveOutput(null, commandLine);
    }

    public static Command saveOutput(Map<String, String> env, String ... commandLine)
    {
        return new SaveOutput(env, commandLine);
    }

    public static Command printOutput(PrintStream output, String ... commandLine)
    {
        return new PrintOutput(null, output, commandLine);
    }

    public static Command printOutput(Map<String, String> env, PrintStream output, String ... commandLine)
    {
        return new PrintOutput(env, output, commandLine);
    }

    public static Command logOutput(Logger log, LogLevel level, String ... commandLine)
    {
        return new LogOutput(null, log, level, commandLine);
    }

    public static Command logOutput(Map<String, String> env, Logger log, LogLevel level, String ... commandLine)
    {
        return new LogOutput(env, log, level, commandLine);
    }

    public int run() throws IOException, Exception
    {
        process = Runtime.getRuntime().exec(commandLine, env, null);
        stdoutConsumer = handleOutput("stdout", reader(process.getInputStream()));
        stderrConsumer = handleOutput("stderr", reader(process.getErrorStream()));
        state = State.RUNNING;
        return status();
    }

    public int run(String stdin) throws IOException, Exception
    {
        process = Runtime.getRuntime().exec(commandLine, env, null);
        stdoutConsumer = handleOutput("stdout", reader(process.getInputStream()));
        stderrConsumer = handleOutput("stderr", reader(process.getErrorStream()));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        writer.write(stdin);
        writer.close();
        state = State.RUNNING;
        return status();
    }

    public void sendInput(String input) throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        writer.write(input);
        writer.close();
    }

    public int status() throws Command.Exception
    {
        waitToComplete();
        return processExit;
    }

    public List<String> stdout() throws Command.Exception
    {
        waitToComplete();
        return stdoutConsumer.output();
    }

    public List<String> stderr() throws Command.Exception
    {
        waitToComplete();
        return stderrConsumer.output();
    }

    public void kill()
    {
        process.destroy();
    }

    protected Command(Map<String, String> env, String ... commandLine)
    {
        this.commandLine = commandLine;
        this.output = null;
        this.log = null;
        if (env == null) {
            this.env = null;
        } else {
            this.env = new String[env.size()];
            int i = 0;
            for (Map.Entry<String, String> entry : env.entrySet()) {
                this.env[i++] = String.format("%s=%s", entry.getKey(), entry.getValue());
            }
        }
    }

    private BufferedReader reader(InputStream input)
    {
        return new BufferedReader(new InputStreamReader(input));
    }

    private void waitToComplete() throws Exception
    {
        if (state == State.NOT_YET_RUN) {
            throw new Command.Exception(String.format("Command has not yet been run: %s", description()));
        }
        try {
            processExit = process.waitFor();
            stdoutConsumer.join();
            stderrConsumer.join();
        } catch (InterruptedException e) {
            logger.error("Caught InterruptedException while waiting for process to complete.", e);
        }
        state = State.ENDED;
    }

    protected abstract Consumer handleOutput(String label, BufferedReader reader);

    private String description()
    {
        StringBuilder buffer = new StringBuilder();
        for (String s : commandLine) {
            buffer.append(s);
            buffer.append(" ");
        }
        return buffer.toString();
    }

    private static final Logger logger = LoggerFactory.getLogger(Command.class);

    private final String[] env;
    private final String[] commandLine;
    private final PrintStream output;
    private final Logger log;
    private State state = State.NOT_YET_RUN;
    private Process process;
    private int processExit;
    private Consumer stdoutConsumer;
    private Consumer stderrConsumer;

    enum State
    {
        NOT_YET_RUN, RUNNING, ENDED
    }

    private static abstract class Consumer extends Thread
    {
        @Override
        public void run()
        {
            String line;
            try {
                while ((line = input.readLine()) != null) {
                    handleLine(line);
                }
            } catch (IOException e) {
                termination = e;
            }
        }

        public List<String> output()
        {
            assert false;
            return null;
        }

        public java.lang.Exception termination()
        {
            return termination;
        }

        public abstract void handleLine(String line);

        protected Consumer(BufferedReader input)
        {
            this.input = input;
        }

        private final BufferedReader input;
        private java.lang.Exception termination;
    }

    private static class SaveOutput extends Command
    {
        SaveOutput(Map<String, String> env, String ... commandLine)
        {
            super(env, commandLine);
        }

        @Override
        protected Consumer handleOutput(String label, BufferedReader reader)
        {
            Consumer consumer = new SaveOutputConsumer(reader);
            consumer.setDaemon(true);
            consumer.start();
            return consumer;
        }

        private class SaveOutputConsumer extends Consumer
        {
            @Override
            public void handleLine(String line)
            {
                output.add(line);
            }

            public SaveOutputConsumer(BufferedReader input)
            {
                super(input);
            }

            public List<String> output()
            {
                return output;
            }

            private final List<String> output = new ArrayList<String>();
        }
    }

    private static class PrintOutput extends Command
    {
        PrintOutput(Map<String, String> env, PrintStream output, String ... commandLine)
        {
            super(env, commandLine);
            this.output = output;
        }

        @Override
        protected Consumer handleOutput(String label, BufferedReader reader)
        {
            Consumer consumer = new PrintOutputConsumer(reader, label);
            consumer.setDaemon(true);
            consumer.start();
            return consumer;
        }

        private final PrintStream output;

        private class PrintOutputConsumer extends Consumer
        {
            @Override
            public void handleLine(String line)
            {
                output.println(String.format("%s: %s", label, line));
            }

            public PrintOutputConsumer(BufferedReader input, String label)
            {
                super(input);
                this.label = label;
            }

            private final String label;
        }
    }

    private static class LogOutput extends Command
    {
        LogOutput(Map<String, String> env, Logger log, LogLevel level, String ... commandLine)
        {
            super(env, commandLine);
            this.log = log;
            if (level == null) {
                log.error("level is none; assuming error");
                this.level = LogLevel.ERROR;
            }
            else {
                this.level = level;
            }
        }

        @Override
        protected Consumer handleOutput(String label, BufferedReader reader)
        {
            Consumer consumer = new LogOutputConsumer(reader, label);
            consumer.setDaemon(true);
            consumer.start();
            return consumer;
        }

        private final Logger log;
        private final LogLevel level;

        private class LogOutputConsumer extends Consumer
        {
            private final static String MESSAGE_FORMAT = "{}: {}";
            @Override
            public void handleLine(String line)
            {
                switch (level) {

//                    case Level.DEBUG_INT: log.debug(message); break;
//                    //Project Godot disabled temporarily
//		    //case Level.TRACE_INT: log.trace(message); break;
//                    case Level.INFO_INT: log.info(message); break;
//                    case Level.WARN_INT: log.warn(message); break;
//                    case Level.ERROR_INT: log.error(message); break;
//                    case Level.FATAL_INT: log.fatal(message); break;
//                    default: assert false; break;
                    case TRACE:
                        log.trace(MESSAGE_FORMAT, label, line);
                        break;
                    case DEBUG:
                        log.debug(MESSAGE_FORMAT, label, line);
                        break;
                    case INFO:
                        log.info(MESSAGE_FORMAT, label, line);
                        break;
                    case WARN:
                        log.warn(MESSAGE_FORMAT, label, line);
                        break;
                    default:
                        log.error("Unknown log level {}. Assuming ERROR", level);
                    case ERROR:
                        log.error(MESSAGE_FORMAT, label, line);
                        break;
                }
            }

            public LogOutputConsumer(BufferedReader input, String label)
            {
                super(input);
                this.label = label;
            }

            private final String label;
        }
    }

    public static class Exception extends java.lang.Exception
    {
        public Exception(String message)
        {
            super(message);
        }

        public Exception(java.lang.Exception cause)
        {
            super(cause);
        }
    }
}
