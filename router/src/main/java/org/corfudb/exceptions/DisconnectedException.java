package org.corfudb.exceptions;

/**
 * Created by mwei on 11/25/16.
 */
public class DisconnectedException extends NetworkException {
    public DisconnectedException(String endpoint) {
        super("Attempted to send a message but the endpoint(" +
                endpoint +") is disconnected!", endpoint);
    }
}
