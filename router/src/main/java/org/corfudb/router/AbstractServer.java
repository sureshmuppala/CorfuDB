package org.corfudb.router;

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

}
