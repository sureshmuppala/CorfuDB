package org.corfudb.router.netty;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.router.IChannel;

/**
 * Created by mwei on 11/29/16.
 */
public class NettyChannelWrapper<M> implements IChannel<M> {

    private final ChannelHandlerContext ctx;

    public NettyChannelWrapper(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void sendMessage(M message) {
        ctx.writeAndFlush(message);
    }
}
