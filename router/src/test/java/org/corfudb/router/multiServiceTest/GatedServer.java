package org.corfudb.router.multiServiceTest;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import org.corfudb.router.AbstractPreconditionServer;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.PreconditionServerMsgHandler;

import java.lang.invoke.MethodHandles;

import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.ERROR_WRONG_PASSWORD;
import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.GATED_REQUEST;
import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.GATED_RESPONSE;

/**
 * Created by mwei on 11/27/16.
 */
public class GatedServer extends AbstractPreconditionServer<MultiServiceMsg<?>, MultiServiceMsgType>  {

    @Getter
    private final PreconditionServerMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType>
            preconditionMsgHandler =
            new PreconditionServerMsgHandler<>(this)
            .generateHandlers(MethodHandles.lookup(), this,
                 MultiServiceServerHandler.class, MultiServiceServerHandler::type);

    public GatedServer(final IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> router,
                       final GatewayServer gatewayServer)
    {
        // Only accept messages where the password is that of the supplied gateway server.
        super(router, (msg, ctx, r) ->  {
            if (gatewayServer.getGatewayPassword().equals(msg.getPassword())) return true;
            r.sendResponse(ctx, msg, ERROR_WRONG_PASSWORD.getVoidMsg());
            return false;
        });
    }

    @MultiServiceServerHandler(type=GATED_REQUEST)
    MultiServiceMsg<String> gatedResponse(MultiServiceMsg<String> msg, ChannelHandlerContext ctx) {
        return GATED_RESPONSE.getPayloadMsg(msg.getPayload());
    }

}
