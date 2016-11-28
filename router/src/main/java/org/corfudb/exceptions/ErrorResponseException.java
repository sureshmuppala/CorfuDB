package org.corfudb.exceptions;

import lombok.Getter;
import org.corfudb.router.IRespondableMsgType;
import org.corfudb.router.IRoutableMsg;

/**
 * Created by mwei on 11/28/16.
 */
public class ErrorResponseException extends RuntimeException {

    @Getter
    final IRoutableMsg errorMessage;

    public ErrorResponseException(IRoutableMsg msg) {
        super("Server returned " + msg.getMsgType().toString());
        this.errorMessage = msg;
    }


    public IRespondableMsgType getErrorType() {
        return (IRespondableMsgType) errorMessage.getMsgType();
    }

    @SuppressWarnings("unchecked")
    public <T extends IRespondableMsgType> T getErrorType(Class<T> type) {
        return (T) errorMessage.getMsgType();
    }
}
