package com.minirpc.registry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ZookeeperRegistryCenter implements RegistryCenter {
    // 默认连接本机 ZooKeeper。Day5 学习阶段先用单机 2181 端口。
    private static final String DEFAULT_CONNECT_STRING = "127.0.0.1:2181";
    // 允许通过 JVM 参数覆盖 ZooKeeper 地址。
    // 例如：-Drpc.registry.zk.connect-string=127.0.0.1:2181
    private static final String CONNECT_STRING_PROPERTY = "rpc.registry.zk.connect-string";
    // 注册中心根路径。所有服务节点都挂在这个路径下面。
    private static final String DEFAULT_BASE_PATH = "/mini-rpc/services";
    private static final String BASE_PATH_PROPERTY = "rpc.registry.zk.base-path";
    // 连接超时与会话超时保持简单可配，便于 Day5 调试。
    private static final int DEFAULT_SESSION_TIMEOUT_MS = 15_000;
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 5_000;

    private final String basePath;
    private final CuratorFramework client;

    public ZookeeperRegistryCenter() {// 无参构造器把默认值准备好，然后直接委托给有参构造器去完成真正初始化
        // 如果你没有手动传参数，
        // 那我就自己先去拿默认的 ZooKeeper 地址和默认根路径，
        // 然后调用“另一个构造器”继续完成真正初始化。
        this(// 调用当前类的另一个构造器
                System.getProperty(CONNECT_STRING_PROPERTY, DEFAULT_CONNECT_STRING),// - 去 JVM 系统属性里找： rpc.registry.zk.connect-string ， 如果没找到，就用默认值：127.0.0.1:2181
                System.getProperty(BASE_PATH_PROPERTY, DEFAULT_BASE_PATH)
        );
    }
    //这叫：
    //- 构造器重载
    //- 构造器委托
    //- 构造器复用

    //这个构造器适合谁用？
    //- 调用方不想手动传参数
    //- 希望系统自己用默认配置启动

    public ZookeeperRegistryCenter(String connectString, String basePath) {// 真正初始化对象
        // connectString 说的是“ZooKeeper 服务在哪”，例如 127.0.0.1:2181。
        String finalConnectString = Objects.requireNonNull(connectString, "connectString");// 如果传进来是 null ，就立刻报错。
        // basePath 说的是“我们项目自己的根目录在哪”，避免和别的业务节点混在一起。默认根路径：/mini-rpc/services
        this.basePath = normalizeBasePath(Objects.requireNonNull(basePath, "basePath"));// 把 ZooKeeper 根路径规范化
        this.client = CuratorFrameworkFactory.builder()// 按照你提供的连接地址、超时配置、重试策略，创建一个 Curator 客户端对象， 我要创建一个 Java 里的 ZooKeeper 客户端，以后用它和 ZooKeeper 通信
                .connectString(finalConnectString)
                .sessionTimeoutMs(Integer.getInteger("rpc.registry.zk.session-timeout-ms", DEFAULT_SESSION_TIMEOUT_MS))
                .connectionTimeoutMs(Integer.getInteger("rpc.registry.zk.connection-timeout-ms", DEFAULT_CONNECTION_TIMEOUT_MS))
                // Day5 先用最小可用重试策略：连不上 ZooKeeper 时，Curator 自己做几次简单重试。
                .retryPolicy(new ExponentialBackoffRetry(1_000, 3))
                .build();
        startClient();// 让 Curator 客户端开始连接 ZooKeeper
    }
    //注意：
    //Java 要保证对象构造链非常明确，
    //要么先调本类另一个构造器，
    //要么先调父类构造器，
    //不能半路插别的逻辑

    @Override
    public void register(ServiceInstance instance) {
        try {
            // 先确保 /mini-rpc/services 和 /mini-rpc/services/HelloService 这类父节点存在。这两个是持久节点
            ensurePersistentNode(basePath);
            ensurePersistentNode(servicePath(instance.getServiceName()));// serviceName 变成 ZooKeeper 目录; host:port 变成目录下的一个子节点
            String instancePath = instancePath(instance);
            if (client.checkExists().forPath(instancePath) != null) {
                // 同一个实例重复注册时直接跳过，避免报“节点已存在”。
                return;
            }
            // 实例节点使用临时节点（EPHEMERAL）：
            // 一旦服务端进程挂掉、ZooKeeper 会话断开，节点会自动消失。
            client.create().withMode(CreateMode.EPHEMERAL).forPath(instancePath);// EPHEMERAL:兜底清理，防止脏节点
            // 临时节点意味着：
            //- 服务端还活着，节点就在
            //- 服务端挂了，节点自动消失
            //- 注册中心不再完全依赖手动注销，
            //- 它能借助 ZooKeeper 会话自动感知实例是否还活着
        } catch (Exception e) {
            throw new RuntimeException("注册 ZooKeeper 服务实例失败: " + instance, e);
        }
    }

    @Override
    public void unregister(ServiceInstance instance) {
        try {
            String instancePath = instancePath(instance);
            if (client.checkExists().forPath(instancePath) == null) {
                return;
            }
            client.delete().forPath(instancePath);
        } catch (Exception e) {
            throw new RuntimeException("注销 ZooKeeper 服务实例失败: " + instance, e);
        }
    }

    @Override
    public List<ServiceInstance> discover(String serviceName) {
        String servicePath = servicePath(serviceName);// 拼出服务路径
        try {
            if (client.checkExists().forPath(servicePath) == null) {// 这个服务还没注册
                return List.of();// 直接返回空列表
            }
            // ZooKeeper 里存的是一组子节点名，例如：
            // /mini-rpc/services/com.xxx.HelloService/127.0.0.1:9000
            List<String> children = client.getChildren().forPath(servicePath);// 读取我们拼出的服务路径下的全部子节点
            List<ServiceInstance> instances = new ArrayList<>();
            for (String child : children) {
                ServiceInstance instance = parseInstance(serviceName, child);// 把子节点名还原成 ServiceInstance
                if (instance != null) {
                    instances.add(instance);
                }
            }
            return instances;// 返回全部的实例
        } catch (KeeperException.NoNodeException ignored) {
            return List.of();
        } catch (Exception e) {
            throw new RuntimeException("从 ZooKeeper 发现服务实例失败: " + serviceName, e);
        }
    }

    @Override
    public void close() {
        // 关闭 Curator 客户端，相当于断开和 ZooKeeper 的会话。
        // 如果当前客户端创建过临时节点，这些节点也会随会话结束而被删除。
        client.close();
    }

    private void startClient() {
        try {
            client.start();
            // 构造时就阻塞等一下，确保后续 register/discover 不是在“未连上 ZooKeeper”的状态下执行。
            boolean connected = client.blockUntilConnected(
                    Integer.getInteger("rpc.registry.zk.connection-timeout-ms", DEFAULT_CONNECTION_TIMEOUT_MS),
                    TimeUnit.MILLISECONDS
            );
            if (!connected) {
                client.close();
                throw new RuntimeException("连接 ZooKeeper 超时: " + client.getZookeeperClient().getCurrentConnectionString());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            client.close();
            throw new RuntimeException("连接 ZooKeeper 时线程被中断", e);
        }
    }

    private void ensurePersistentNode(String path) throws Exception {
        if (client.checkExists().forPath(path) != null) {
            return;
        }
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
    }

    private String servicePath(String serviceName) {
        return basePath + "/" + serviceName;
    }

    private String instancePath(ServiceInstance instance) {
        return servicePath(instance.getServiceName()) + "/" + instance.endpointKey();
    }

    private String normalizeBasePath(String path) {
        String normalized = path.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("ZooKeeper basePath 不能为空");
        }
        return normalized.startsWith("/") ? normalized : "/" + normalized;
    }

    private ServiceInstance parseInstance(String serviceName, String childNodeName) {
        int separatorIndex = childNodeName.lastIndexOf(':');
        if (separatorIndex <= 0 || separatorIndex == childNodeName.length() - 1) {
            return null;
        }
        String host = childNodeName.substring(0, separatorIndex);
        int port = Integer.parseInt(childNodeName.substring(separatorIndex + 1));
        return new ServiceInstance(serviceName, host, port);
    }
}
