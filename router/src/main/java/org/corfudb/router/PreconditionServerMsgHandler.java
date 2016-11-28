package org.corfudb.router;

import io.netty.channel.ChannelHandlerContext;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.util.function.Function;

/**
 * Created by mwei on 11/27/16.
 */
public class PreconditionServerMsgHandler<M extends IRoutableMsg<T>, T> extends
    ServerMsgHandler<M,T> {

    final IPreconditionServer<M,T> preconditionServer;

    /** Construct a new instance of ServerMsgHandler. */
    public PreconditionServerMsgHandler(IPreconditionServer<M,T> server) {
        super(server);
        this.preconditionServer = server;
    }

    /**
     * Handle an incoming routable message.
     *
     * @param message The message to handle.
     * @param ctx     The channel handler context.
     * @return True, if the message was handled.
     * False otherwise.
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean handle(M message, ChannelHandlerContext ctx) {
        // check if the message meets the precondition first.
        if (preconditionServer.getPreconditionFunction().check(message,
               ctx, preconditionServer.getRouter())) {
            return super.handle(message, ctx);
        } else {
            return false;
        }
    }

    /**
     * Generate handlers for a particular server.
     *
     * @param caller             The context that is being used. Call MethodHandles.lookup() to obtain.
     * @param o                  The object that implements the server.
     * @param annotationType
     * @param typeFromAnnotation @return
     */
    @Override
    public <A extends Annotation> PreconditionServerMsgHandler<M, T> generateHandlers(MethodHandles.Lookup caller, Object o, Class<A> annotationType, Function<A, T> typeFromAnnotation) {
        super.generateHandlers(caller, o, annotationType, typeFromAnnotation);
        return this;
    }
}
