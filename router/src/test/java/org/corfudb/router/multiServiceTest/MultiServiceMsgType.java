package org.corfudb.router.multiServiceTest;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.router.IRespondableMsgType;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 11/26/16.
 */
@RequiredArgsConstructor
public enum MultiServiceMsgType implements IRespondableMsgType {
    ECHO_REQUEST(0, false, String.class),
    ECHO_RESPONSE(1, true, String.class),
    DISCARD(2, false, String.class),
    GATEWAY_REQUEST(3, false, Void.class),
    GATEWAY_RESPONSE(4, true, String.class),
    GATED_REQUEST(5, false, String.class),
    GATED_RESPONSE(6, true, String.class),
    ERROR_WRONG_PASSWORD(7, true, Void.class)
    ;

    @Getter
    final int value;

    @Getter
    final boolean response;

    @Getter
    final Class<?> payloadType;

    @Getter(lazy=true)
    private final boolean error = determineIsError();

    private boolean determineIsError() {
        return this.toString().startsWith("ERROR_");
    }

    <T> MultiServiceMsg<T> getPayloadMsg(T payload) {
        if (payloadType.isInstance(payload)) {
            return new MultiServiceMsg<T>(this, payload);
        }
        throw new ClassCastException("Wrong type for payload!");
    }

    MultiServiceMsg<Void> getVoidMsg() {
        return new MultiServiceMsg<>(this, null);
    }

    static Map<Integer, MultiServiceMsgType> typeMap =
            Arrays.stream(MultiServiceMsgType.values())
                    .collect(Collectors.toMap(MultiServiceMsgType::getValue, Function.identity()));
}
