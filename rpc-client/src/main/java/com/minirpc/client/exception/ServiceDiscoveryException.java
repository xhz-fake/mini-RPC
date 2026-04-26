package com.minirpc.client.exception;

public class ServiceDiscoveryException extends RpcClientException {// 连“可调哪个实例”都没找明白
    public ServiceDiscoveryException(String message) {
        super(message);
    }

    public ServiceDiscoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
