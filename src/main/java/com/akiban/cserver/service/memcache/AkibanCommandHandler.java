package com.akiban.cserver.service.memcache;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.protocol.Command;
import com.thimbleware.jmemcached.protocol.CommandMessage;
import com.thimbleware.jmemcached.protocol.ResponseMessage;
import com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException;

import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.nio.ByteBuffer;

import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.store.Store;


/**
 * Processes CommandMessage and generate ResponseMessage, shared among all channels.
 *
 * Inspried by: com.thimbleware.jmemcached.protocol.MemcachedCommandHandler
 */
@ChannelHandler.Sharable
public final class AkibanCommandHandler extends SimpleChannelUpstreamHandler
{
    /**
     * State variables that are universal for entire service.
     * The handler *must* be declared with a ChannelPipelineCoverage of "all".
     */
    private final Store store;
    private final DefaultChannelGroup channelGroup;
    private static final Log LOG = LogFactory.getLog(MemcacheService.class);


    public AkibanCommandHandler(Store store, DefaultChannelGroup channelGroup) {
        this.store = store;
        this.channelGroup = channelGroup;
    }

    /**
     * On open we manage some statistics, and add this connection to the channel group.
     */
    @Override
    public void channelOpen(ChannelHandlerContext context, ChannelStateEvent event) throws Exception {
        ByteBuffer payload = ByteBuffer.allocate(65536);
        context.setAttachment(payload);
        channelGroup.add(context.getChannel());
    }

    /**
     * Track stats and then remove from channel group
     */
    @Override
    public void channelClosed(ChannelHandlerContext context, ChannelStateEvent event) throws Exception {
        context.setAttachment(null);
        channelGroup.remove(context.getChannel());
    }
    
    /**
     * Eat the exception, probably an improperly closed client.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
    {
        LOG.warn("exceptionCaught: " + e);
    }

    /**
     * Turn CommandMessages into executions against the CS and then pass on downstream message
     */
    @Override
    @SuppressWarnings("unchecked")
    public void messageReceived(ChannelHandlerContext context, MessageEvent event) throws Exception {
        if (!(event.getMessage() instanceof CommandMessage)) {
            // Ignore what this encoder can't encode.
            context.sendUpstream(event);
            return;
        }

        CommandMessage<CacheElement> command = (CommandMessage<CacheElement>) event.getMessage();
        Command cmdOp = command.cmd;

        if(LOG.isDebugEnabled()) {
            StringBuilder msg = new StringBuilder();
            msg.append(command.cmd);
            if(command.element != null) {
                msg.append(" ").append(command.element.getKeystring());
            }
            for(int i = 0; i < command.keys.size(); ++i) {
                msg.append(" ").append(command.keys.get(i));
            }
            LOG.debug(msg.toString());
        }

        Channel channel = event.getChannel();
        switch(cmdOp) {
            case GET:
            case GETS:      handleGets(context, command, channel);      break;
            case SET:       handleSet(context, command, channel);       break;
            case CAS:       handleCas(context, command, channel);       break;
            case ADD:       handleAdd(context, command, channel);       break;
            case REPLACE:   handleReplace(context, command, channel);   break;
            case APPEND:    handleAppend(context, command, channel);    break;
            case PREPEND:   handlePrepend(context, command, channel);   break;
            case INCR:      handleIncr(context, command, channel);      break;
            case DECR:      handleDecr(context, command, channel);      break;
            case DELETE:    handleDelete(context, command, channel);    break;
            case STATS:     handleStats(context, command, channel);     break;
            case VERSION:   handleVersion(context, command, channel);   break;
            case QUIT:      handleQuit(channel);                        break;
            case FLUSH_ALL: handleFlush(context, command, channel);     break;
            default:
                if(cmdOp == null) {
                    handleNoOp(context, command);
                }
                else {
                    throw new UnknownCommandException("unknown command:" + cmdOp);
                }
        }
    }

    protected void handleNoOp(ChannelHandlerContext context, CommandMessage<CacheElement> command) {
        Channels.fireMessageReceived(context, new ResponseMessage(command));
    }

    protected void handleFlush(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        boolean flushSuccess = false;
        // flushSuccess = cache.flush_all(command.time)
        Channels.fireMessageReceived(context, new ResponseMessage(command).withFlushResponse(flushSuccess), channel.getRemoteAddress());
    }

    protected void handleQuit(Channel channel) {
        channel.disconnect();
    }

    protected void handleVersion(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        ResponseMessage responseMessage = new ResponseMessage(command);
        responseMessage.version = MemCacheDaemon.memcachedVersion;
        Channels.fireMessageReceived(context, responseMessage, channel.getRemoteAddress());
    }

    protected void handleStats(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
//        String option = "";
//        if(command.keys.size() > 0) {
//            option = new String(command.keys.get(0));
//        }
        Map<String, Set<String>> statResponse = null;
        // statResponse = cache.stat(option)
        Channels.fireMessageReceived(context, new ResponseMessage(command).withStatResponse(statResponse), channel.getRemoteAddress());
    }

    protected void handleDelete(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        Cache.DeleteResponse dr = null;
        //dr = cache.delete(command.keys.get(0), command.time);
        Channels.fireMessageReceived(context, new ResponseMessage(command).withDeleteResponse(dr), channel.getRemoteAddress());
    }

    protected void handleDecr(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        Integer incrDecrResp = null;
        //incDecrResp = cache.get_add(command.keys.get(0), -1 * command.incrAmount);
        Channels.fireMessageReceived(context, new ResponseMessage(command).withIncrDecrResponse(incrDecrResp), channel.getRemoteAddress());
    }

    protected void handleIncr(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        Integer incrDecrResp = null;
        //incRecrResp = cache.get_add(command.keys.get(0), command.incrAmount); // TODO support default value and expiry!!
        Channels.fireMessageReceived(context, new ResponseMessage(command).withIncrDecrResponse(incrDecrResp), channel.getRemoteAddress());
    }

    protected void handlePrepend(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        Cache.StoreResponse ret = null;
        //ret = cache.prepend(command.element);
        Channels.fireMessageReceived(context, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleAppend(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        Cache.StoreResponse ret = null;
        //ret = cache.append(command.element);
        Channels.fireMessageReceived(context, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleReplace(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        Cache.StoreResponse ret = null;
        //ret = cache.replace(command.element);
        Channels.fireMessageReceived(context, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleAdd(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        Cache.StoreResponse ret = null;
        //ret = cache.add(command.element);
        Channels.fireMessageReceived(context, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleCas(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        Cache.StoreResponse ret = null;
        //ret = cache.cas(command.cas_key, command.element);
        Channels.fireMessageReceived(context, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleSet(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        Cache.StoreResponse ret = null;
        //ret = cache.set(command.element);
        Channels.fireMessageReceived(context, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleGets(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        String[] keys = new String[command.keys.size()];
        keys = command.keys.toArray(keys);
        CacheElement[] results = null;

        byte[] key = keys[0].getBytes();
        byte[] result_bytes = null;

        if(key != null) {
            String request = new String(key);
            String[] tokens = request.split(":");

            if(tokens.length == 3 || tokens.length == 4) {
                String schema = tokens[0];
                String table = tokens[1];
                String colkey = tokens[2];
                String min_val = null;
                String max_val = null;
                ByteBuffer payload = (ByteBuffer) context.getAttachment();

                if(tokens.length == 4) {
                    min_val = max_val = tokens[3];
                }

                final RowDefCache cache = store.getRowDefCache();

                try {
                    StringBuilder sb = new StringBuilder();
                    List<RowData> list = store.fetchRows(schema, table, colkey, min_val, max_val, null, payload);
                    
                    int current_def_id = -1;
                    List<Integer> def_id_stack = new ArrayList<Integer>();

                    for(RowData data : list) {
                        final int def_id = data.getRowDefId();
                        final RowDef def = cache.getRowDef(def_id);
                        final int parent_def_id = def.getParentRowDefId();

                        if(def_id_stack.isEmpty()) {
                            current_def_id = def_id;
                            def_id_stack.add(parent_def_id);
                            sb.append("{ \"");
                            sb.append(def.getTableName());
                            sb.append("\" : ");
                            if(min_val == null) {
                                sb.append(" [ ");
                            }
                        }
                        else if(def_id == current_def_id) {
                            // another leaf on current branch (add to current open array)
                            sb.append(" }, ");
                        }
                        else if(parent_def_id == current_def_id) {
                            // down the tree, new branch (new open array)
                            current_def_id = def_id;
                            def_id_stack.add(parent_def_id);
                            
                            sb.append(", \"");
                            sb.append(def.getTableName());
                            sb.append("\" : [ ");
                        }
                        else {
                            // a) sibling branch or b) up the tree to an old branch (close array for each step up)
                            current_def_id = def_id;
                            int pop_count = 0;
                            int last = def_id_stack.size() - 1;

                            sb.append(" } ]");
                            while(!def_id_stack.get(last).equals(parent_def_id)) {
                                if(pop_count++ > 0) {
                                    sb.append(" ]");
                                }
                                sb.append(" }");
                                def_id_stack.remove(last--);
                            }
                            
                            if(pop_count == 0) {
                                // Was sibling
                                sb.append(", \"");
                                sb.append(def.getTableName());
                                sb.append("\" : [ ");
                            }
                            else {
                                // Was child
                                sb.append(", ");
                            }
                        }

                        String json_row = data.toJSONString(cache);
                        sb.append("{ ");
                        sb.append(json_row);
                    }

                    if(sb.length() > 0) {
                        int last = def_id_stack.size() - 1;
                        while(last > 0) {
                            sb.append(" } ]");
                            def_id_stack.remove(last--);
                        }
                        sb.append(" }");
                        if(min_val == null) {
                            sb.append(" ]");
                        }
                        sb.append(" }");
                        result_bytes = sb.toString().getBytes();
                    }
                }
                catch(Exception e) {
                    result_bytes = ("read error: " + e.getMessage()).getBytes();
                }
            }
            else {
                result_bytes = ("invalid key: " + request).getBytes();
            }
        }

        if(result_bytes != null) {
            LocalCacheElement element = new LocalCacheElement(keys[0]);
            element.setData(result_bytes);
            results = new CacheElement[] { element };
        }

        ResponseMessage<CacheElement> resp = new ResponseMessage<CacheElement>(command).withElements(results);
        Channels.fireMessageReceived(context, resp, channel.getRemoteAddress());
    }
}
