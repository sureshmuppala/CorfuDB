package org.corfudb.router.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.function.Function;

/**
 * Created by mwei on 11/25/16.
 */
public class NettyMsgDecoder<M> extends ByteToMessageDecoder {

    private final Function<ByteBuf, M> deserializerFunction;

    public NettyMsgDecoder(Function<ByteBuf, M> deserializerFunction) {
        this.deserializerFunction = deserializerFunction;
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        list.add(deserializerFunction.apply(byteBuf));
    }

    @Override
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in,
                              List<Object> out) throws Exception {
        if (in != Unpooled.EMPTY_BUFFER) {
            this.decode(ctx, in, out);
        }
    }
}
