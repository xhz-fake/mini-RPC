package com.minirpc.registry;

import java.util.Objects;

public final class ServiceInstance {// 是一个 地址说明书
    // serviceName 说的是“我是谁”，例如 HelloService。
    private final String serviceName;
    // host + port 说的是“我在哪”，例如 127.0.0.1:9000。
    private final String host;
    private final int port;

    public ServiceInstance(String serviceName, String host, int port) {
        this.serviceName = Objects.requireNonNull(serviceName, "serviceName");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    // endpointKey 是把地址压缩成一个统一字符串，便于做 Map 的 key。
    public String endpointKey() {
        return host + ":" + port;
    }

    @Override
    // equals 的作用是判断“两个实例是不是同一个实例”。
    // 这里要求 serviceName、host、port 三者都相同，才算同一个实例。
    // 这样做的原因是：Day4 重试时要排除“本次已经试过的实例”，
    // 如果不重写 equals，contains(instance) 判断时就可能不准。
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ServiceInstance that)) {
            return false;
        }
        return port == that.port
                && serviceName.equals(that.serviceName)
                && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        // hashCode 要和 equals 保持一致。
        // 只要两个实例被 equals 判定为同一个，它们的 hashCode 也必须相同。
        return Objects.hash(serviceName, host, port);
    }

    @Override
    public String toString() {
        // 打印日志时更友好，能直接看到“服务名@地址”。
        return serviceName + "@" + host + ":" + port;
    }
}
