package org.corfudb.router;

/**
 * Created by mwei on 11/24/16.
 */
public interface IRespondableMsg {

    long getRequestID();

    void setRequestID(long requestID);

    default void copyFieldsToResponse(IRespondableMsg outMsg) {
        outMsg.setRequestID(getRequestID());
    }
}
