package org.corfudb.router.pingTest;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.router.AbstractServer;
import org.corfudb.router.IServer;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.ServerMsgHandler;

import java.lang.invoke.MethodHandles;

/**
 * Created by mwei on 11/23/16.
 */
@Slf4j
public class PingServer extends AbstractServer<PingMsg, PingMsgType> {

    /** Handler for the ping server. */
    @Getter
    private final ServerMsgHandler<PingMsg, PingMsgType> msgHandler =
            new ServerMsgHandler<>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            PingServerHandler.class, PingServerHandler::type);

    public PingServer(IServerRouter<PingMsg, PingMsgType> router) {
        super(router);
    }

    @PingServerHandler(type= PingMsgType.PING)
    public PingMsg handlePing(PingMsg msg, ChannelHandlerContext ctx) {
        return PingMsgType.PONG.getMsg();
    }
}
