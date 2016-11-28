package org.corfudb.router.pingTest;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.router.IRespondableMsgType;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 11/23/16.
 */
@RequiredArgsConstructor
public enum PingMsgType implements IRespondableMsgType {
    PING(0, false),
    PONG(1, true);

    @Getter
    final int value;

    @Getter
    final boolean response;

    public boolean isError() {
        return false;
    }

    PingMsg getMsg() {
        return new PingMsg(this);
    }

    static Map<Integer, PingMsgType> typeMap =
            Arrays.stream(PingMsgType.values())
                    .collect(Collectors.toMap(PingMsgType::getValue, Function.identity()));
}
