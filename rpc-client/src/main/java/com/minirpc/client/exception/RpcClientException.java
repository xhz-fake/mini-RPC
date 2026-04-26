package com.minirpc.client.exception;

public class RpcClientException extends RuntimeException {// 异常基类，一个更通用的客户端兜底异常
    public RpcClientException(String message) {
        super(message);
    }

    public RpcClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
