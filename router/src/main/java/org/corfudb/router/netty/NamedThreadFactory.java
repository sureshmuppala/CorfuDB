package org.corfudb.router.netty;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/** This helper class generates threads with the name "prefix-nn"
 * where nn is a unique number based on the number of times a
 * new thread is requested by the thread factory.
 *
 * Created by mwei on 11/23/16.
 */
public class NamedThreadFactory implements ThreadFactory {

    /** An atomic number which will be issued to the next thread requested. */
    private final AtomicInteger threadNum = new AtomicInteger(0);

    /** The prefix which will be applied each thread requested. */
    private final String prefix;

    NamedThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    /**
     * Constructs a new {@code Thread}.  Implementations may also initialize
     * priority, name, daemon status, {@code ThreadGroup}, etc.
     *
     * @param r a runnable to be executed by new thread instance
     * @return constructed thread, or {@code null} if the request to
     * create a thread is rejected
     */
    @Override
    public Thread newThread(@Nonnull Runnable r) {
        Thread t = new Thread(r);
        t.setName(prefix + "-" + threadNum.getAndIncrement());
        return t;
    }
}
