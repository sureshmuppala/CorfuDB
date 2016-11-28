package org.corfudb.router.pingTest;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.router.IRespondableMsg;
import org.corfudb.router.IRoutableMsg;
import org.corfudb.router.netty.INettyEncodableMsg;

/**
 * Created by mwei on 11/23/16.
 */
public class PingMsg implements IRoutableMsg<PingMsgType>, IRespondableMsg,
        INettyEncodableMsg {

    @Getter
    final PingMsgType msgType;

    @Getter
    @Setter
    long requestID;

    public PingMsg(PingMsgType type) {
        this.msgType = type;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeInt(msgType.getValue());
        buf.writeLong(requestID);
    }

    public static PingMsg decode(ByteBuf buf) {
        PingMsgType type = PingMsgType.typeMap.get(buf.readInt());
        PingMsg m = new PingMsg(type);
        m.setRequestID(buf.readLong());
        return m;
    }
}
