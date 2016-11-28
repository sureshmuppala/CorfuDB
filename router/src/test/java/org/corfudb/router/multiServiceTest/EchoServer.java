package org.corfudb.router.multiServiceTest;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import org.corfudb.router.AbstractServer;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.ServerMsgHandler;

import java.lang.invoke.MethodHandles;

import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.ECHO_RESPONSE;

/**
 * Created by mwei on 11/27/16.
 */
public class EchoServer extends AbstractServer<MultiServiceMsg<?>, MultiServiceMsgType> {

    /** Handler for the ping server. */
    @Getter
    private final ServerMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType> msgHandler =
            new ServerMsgHandler<>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            MultiServiceServerHandler.class, MultiServiceServerHandler::type);

    public EchoServer(IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> router) {
        super(router);
    }

    @MultiServiceServerHandler(type=MultiServiceMsgType.ECHO_REQUEST)
    MultiServiceMsg<String> echo(MultiServiceMsg<String> message, ChannelHandlerContext ctx) {
        return ECHO_RESPONSE.getPayloadMsg(message.getPayload());
    }
}
