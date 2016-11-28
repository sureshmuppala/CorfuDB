package org.corfudb.router;

import lombok.Getter;

import java.util.function.BiFunction;

/**
 * Created by mwei on 11/27/16.
 */
public abstract class AbstractPreconditionServer<M extends IRoutableMsg<T>, T>
        extends AbstractServer<M,T> implements IPreconditionServer<M,T> {

    @Getter
    final PreconditionFunction<M,T> preconditionFunction;

    public abstract PreconditionServerMsgHandler<M,T> getPreconditionMsgHandler();

    @Override
    final public ServerMsgHandler<M,T> getMsgHandler() {
        return getPreconditionMsgHandler();
    }

    public AbstractPreconditionServer(IServerRouter<M,T> router,
        PreconditionFunction<M,T> preconditionFunction) {
        super(router);
        this.preconditionFunction = preconditionFunction;
    }


}
