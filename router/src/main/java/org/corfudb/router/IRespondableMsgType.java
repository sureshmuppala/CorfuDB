package org.corfudb.router;

/**
 * Created by mwei on 11/24/16.
 */
public interface IRespondableMsgType {
    boolean isResponse();
    boolean isError();
}
