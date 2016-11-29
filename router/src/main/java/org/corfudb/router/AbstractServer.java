package org.corfudb.router;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;

/**
 * Created by mwei on 11/23/16.
 */
public abstract class AbstractServer<M extends IRoutableMsg<T>, T> implements IServer<M,T> {

    @Getter
    private final IServerRouter<M,T> router;

    public abstract ServerMsgHandler<M,T> getMsgHandler();

    public AbstractServer(IServerRouter<M,T> router) {
        this.router = router;
    }

    public void sendMessage(ChannelHandlerContext ctx, M msg) {
        router.sendMessage(ctx, msg);
    }

    public void sendResponse(ChannelHandlerContext ctx, IRespondableMsg inMsg, IRespondableMsg outMsg) {
        router.sendResponse(ctx, inMsg, outMsg);
    }
}
