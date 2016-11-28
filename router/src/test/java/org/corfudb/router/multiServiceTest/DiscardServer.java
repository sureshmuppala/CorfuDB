package org.corfudb.router.multiServiceTest;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import org.corfudb.router.AbstractServer;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.ServerMsgHandler;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.ECHO_RESPONSE;

/**
 * Created by mwei on 11/27/16.
 */
public class DiscardServer extends AbstractServer<MultiServiceMsg<?>, MultiServiceMsgType> {

    /** Completed once a message is "discarded". */
    @Getter
    final CompletableFuture<Boolean> discarded = new CompletableFuture<>();

    /** Handler for the discard server. */
    @Getter
    private final ServerMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType> msgHandler =
            new ServerMsgHandler<>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            MultiServiceServerHandler.class, MultiServiceServerHandler::type);

    public DiscardServer(IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> router) {
        super(router);
    }

    @MultiServiceServerHandler(type=MultiServiceMsgType.DISCARD)
    void discard(MultiServiceMsg<String> message, ChannelHandlerContext ctx) {
        discarded.complete(true);
    }
}
