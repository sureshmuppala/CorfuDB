package org.corfudb.exceptions;

import lombok.Getter;

/** Represents a connection caused by a network issue to
 * a remote endpoint.
 * Created by mwei on 11/25/16.
 */
public class NetworkException extends RuntimeException {

    /** The endpoint which caused the network exception. */
    @Getter
    final String endpoint;

    public NetworkException(String endpoint) {
        super("General Network failure connecting to endpoint " + endpoint);
        this.endpoint = endpoint;
    }

    public NetworkException(String endpoint, Throwable cause) {
        super("General Network failure connecting to endpoint " + endpoint, cause);
        this.endpoint = endpoint;
    }

    public NetworkException(String message, String endpoint) {
        super(message);
        this.endpoint = endpoint;
    }

    public NetworkException(String message, String endpoint, Throwable cause) {
        super(message, cause);
        this.endpoint = endpoint;
    }
}
