package com.minirpc.client.exception;

public class RpcConnectionException extends RpcClientException {// 找到实例了，但连不上
    public RpcConnectionException(String message) {
        super(message);
    }

    public RpcConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
