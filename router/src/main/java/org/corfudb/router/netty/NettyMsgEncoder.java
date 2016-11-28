package org.corfudb.router.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by mwei on 11/25/16.
 */
@Slf4j
public class NettyMsgEncoder<M extends INettyEncodableMsg> extends MessageToByteEncoder {

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext,
                          Object msg,
                          ByteBuf byteBuf) throws Exception {
        try {
            ((M)msg).encode(byteBuf);
        } catch (Exception e) {
            log.error("Error during serialization!", e);
        }
    }
}
