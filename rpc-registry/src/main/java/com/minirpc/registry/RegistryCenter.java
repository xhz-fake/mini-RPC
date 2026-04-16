package com.minirpc.registry;

import java.util.List;

public interface RegistryCenter extends AutoCloseable {
    // register 的意思不是“把业务代码上传到某处”，
    // 而是把“某个服务实例现在在哪个地址提供服务”登记出去。
    void register(ServiceInstance instance);

    // 服务下线时把实例地址移除，避免客户端继续发现到一个已经停掉的实例。
    void unregister(ServiceInstance instance);

    // 按服务名查询当前有哪些可用实例。
    // 返回的不是实现类对象，而是一组“地址说明书” ServiceInstance。
    List<ServiceInstance> discover(String serviceName);

    // 对 FileRegistryCenter 来说这里通常什么都不用做；
    // 对 ZookeeperRegistryCenter 来说，这里要负责关闭底层客户端连接。
    @Override
    default void close() {
    }
}
