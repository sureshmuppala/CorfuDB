package org.corfudb.router.pingTest;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.router.*;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 11/23/16.
 */
@Slf4j
public class PingClient extends AbstractRequestClient<PingMsg, PingMsgType> {

    /** Handler for the ping server. */
    @Getter
    private final ClientMsgHandler<PingMsg, PingMsgType> msgHandler =
            new ClientMsgHandler<PingMsg, PingMsgType>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            PingClientHandler.class, PingClientHandler::type);


    public PingClient(IRequestClientRouter<PingMsg, PingMsgType> router) {
        super(router);
    }

    public CompletableFuture<Boolean> ping() {
        return sendMessageAndGetResponse(PingMsgType.PING.getMsg())
                .thenApply(msg -> {
                    if (msg.getMsgType() == PingMsgType.PONG) {
                        return true;
                    }
                    throw new RuntimeException("Unexpected message type " +
                        msg.getMsgType().toString());
                });
    }

}
