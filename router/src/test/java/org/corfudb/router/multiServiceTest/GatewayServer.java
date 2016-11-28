package org.corfudb.router.multiServiceTest;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import org.corfudb.router.AbstractServer;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.ServerMsgHandler;

import java.lang.invoke.MethodHandles;

import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.GATEWAY_RESPONSE;

/**
 * Created by mwei on 11/27/16.
 */
public class GatewayServer extends AbstractServer<MultiServiceMsg<?>, MultiServiceMsgType>  {

    @Getter
    private final String gatewayPassword;


    @Getter
    private final ServerMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType> msgHandler =
            new ServerMsgHandler<>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            MultiServiceServerHandler.class, MultiServiceServerHandler::type);

    public GatewayServer(IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> router,
                         String gatewayPassword) {
        super(router);
        this.gatewayPassword = gatewayPassword;
    }

    @MultiServiceServerHandler(type=MultiServiceMsgType.GATEWAY_REQUEST)
    MultiServiceMsg<String> getPassword(MultiServiceMsg<Void> message, ChannelHandlerContext ctx) {
        return GATEWAY_RESPONSE.getPayloadMsg(gatewayPassword);
    }
}
