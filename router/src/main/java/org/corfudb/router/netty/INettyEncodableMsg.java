package org.corfudb.router.netty;

import io.netty.buffer.ByteBuf;

/** A Netty encodable message, which can be serialized by Netty.
 * Created by mwei on 11/25/16.
 */
public interface INettyEncodableMsg {

    /** Encode the message into a Netty byte buffer.
     *
     * @param buf   The byte buffer to serialize the message into.
     */
    void encode(ByteBuf buf);
}
