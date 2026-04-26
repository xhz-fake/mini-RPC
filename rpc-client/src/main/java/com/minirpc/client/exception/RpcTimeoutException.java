package com.minirpc.client.exception;

public class RpcTimeoutException extends RpcClientException {// 请求发出去了，但等太久没回来
    public RpcTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
