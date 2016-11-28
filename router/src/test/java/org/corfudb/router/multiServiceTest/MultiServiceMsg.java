package org.corfudb.router.multiServiceTest;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.router.IRespondableMsg;
import org.corfudb.router.IRoutableMsg;
import org.corfudb.router.netty.INettyEncodableMsg;

/**
 * Created by mwei on 11/26/16.
 */
public class MultiServiceMsg<T> implements IRoutableMsg<MultiServiceMsgType>,
        IRespondableMsg,
        INettyEncodableMsg {

    @Getter
    @Setter
    long requestID;

    @Getter
    final MultiServiceMsgType msgType;

    @Getter
    final T payload;

    @Getter
    @Setter
    String password = "";

    public MultiServiceMsg(MultiServiceMsgType type, T payload) {
        this.msgType = type;
        this.payload = payload;
    }

    /**
     * Encode the message into a Netty byte buffer.
     *
     * @param buf The byte buffer to serialize the message into.
     */
    @Override
    public void encode(ByteBuf buf) {
        buf.writeInt(msgType.getValue());
        buf.writeLong(requestID);


        if (msgType.getPayloadType() == String.class) {
            byte[] str = ((String)payload).getBytes();
            buf.writeInt(str.length);
            buf.writeBytes(str);
        }

        byte[] str = password.getBytes();
        buf.writeInt(str.length);
        buf.writeBytes(str);
    }


    public static MultiServiceMsg<?> decode(ByteBuf buf) {
        MultiServiceMsgType type = MultiServiceMsgType.typeMap.get(buf.readInt());
        long requestID = buf.readLong();
        Object payload = null;
        if (type.getPayloadType() == String.class) {
           int length =  buf.readInt();
           byte[] str = new byte[length];
           buf.readBytes(str);
           payload = new String(str);
        }
        int pwLength =  buf.readInt();
        byte[] pwStr = new byte[pwLength];
        buf.readBytes(pwStr);


        MultiServiceMsg<?> m = new MultiServiceMsg<>(type, payload);
        m.setRequestID(requestID);
        m.setPassword(new String(pwStr));
        return m;
    }
}
