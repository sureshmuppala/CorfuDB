package org.corfudb.router;

import io.netty.channel.ChannelHandlerContext;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by mwei on 11/27/16.
 */
public interface IPreconditionServer<M extends IRoutableMsg<T>,T> extends IServer<M,T> {

    @FunctionalInterface
    interface PreconditionFunction<M extends IRoutableMsg<T>,T> {
        boolean check(M message, ChannelHandlerContext ctx, IServerRouter<M,T> router);
    }
    PreconditionFunction<M,T> getPreconditionFunction();
}
