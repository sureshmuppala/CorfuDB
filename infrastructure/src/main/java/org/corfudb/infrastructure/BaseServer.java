package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.router.AbstractServer;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.ServerMsgHandler;
import org.corfudb.util.Utils;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer extends AbstractServer<CorfuMsg, CorfuMsgType> {

    /** Options map, if available */
    @Getter
    @Setter
    public Map<String, Object> optionsMap = new HashMap<>();

    /** Handler for the base server */
    @Getter
    private final ServerMsgHandler<CorfuMsg, CorfuMsgType> msgHandler =
            new ServerMsgHandler<CorfuMsg, CorfuMsgType>(this)
            .generateHandlers(MethodHandles.lookup(), this, ServerHandler.class, ServerHandler::type);


    public BaseServer(IServerRouter<CorfuMsg, CorfuMsgType> router) {
        super(router);
    }

    /** Respond to a ping message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     */
    @ServerHandler(type=CorfuMsgType.PING)
    private static CorfuMsg ping(CorfuMsg msg, ChannelHandlerContext ctx) {
        return CorfuMsgType.PONG_RESPONSE.msg();
    }

    /** Respond to a version request message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     */
    @ServerHandler(type=CorfuMsgType.VERSION_REQUEST)
    private JSONPayloadMsg getVersion(CorfuMsg msg, ChannelHandlerContext ctx) {
        VersionInfo vi = new VersionInfo(optionsMap);
        return new JSONPayloadMsg<>(vi, CorfuMsgType.VERSION_RESPONSE);
    }

    /** Reset the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 100, then it restarts
     * the server.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     */
    @ServerHandler(type=CorfuMsgType.RESET)
    private void doReset(CorfuMsg msg, ChannelHandlerContext ctx) {
        log.warn("Remote reset requested from client " + msg.getClientID());
        sendResponse(ctx, msg, CorfuMsgType.ACK_RESPONSE.msg());
        Utils.sleepUninterruptibly(500); // Sleep, to make sure that all channels are flushed...
        System.exit(100);
    }
}
