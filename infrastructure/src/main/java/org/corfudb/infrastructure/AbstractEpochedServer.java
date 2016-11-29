package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.router.AbstractPreconditionServer;
import org.corfudb.router.IServerRouter;

/**
 * Created by mwei on 11/29/16.
 */
public abstract class AbstractEpochedServer extends AbstractPreconditionServer<CorfuMsg, CorfuMsgType> {

    public AbstractEpochedServer(IServerRouter<CorfuMsg, CorfuMsgType> router,
                                 ServerContext context) {
        super(router, (msg, ctx, r) -> {
            if (msg.getEpoch() == context.getServerEpoch())
            {
                return true;
            }
            r.sendResponse(ctx, msg, CorfuMsgType.WRONG_EPOCH_ERROR.msg());
            return false;
        });
    }
}
