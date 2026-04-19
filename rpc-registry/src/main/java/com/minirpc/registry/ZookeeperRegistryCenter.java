package com.minirpc.registry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ZookeeperRegistryCenter implements RegistryCenter {// 负责连接 ZooKeeper、注册/注销服务实例、发现服务实例，并在客户端侧维护“本地实例缓存 + watcher 自动刷新”
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
    private final CuratorFramework client;// 它是 Curator 提供的 ZooKeeper 客户端主接口
    // Day6：把“服务名 -> 实例列表”缓存在当前 JVM 里。
    // 这样客户端后续再 discover 时，就不必每次都去 ZooKeeper 重新查一遍。
    private final Map<String, List<ServiceInstance>> serviceCache = new ConcurrentHashMap<>();
    // Day6：每个服务名维护一个 CuratorCache 监听器。
    // 它的作用是盯住 ZooKeeper 对应路径，一旦子节点变化，就把本地缓存刷新掉。
    private final Map<String, CuratorCache> serviceWatchers = new ConcurrentHashMap<>();
    // 避免并发 discover 时，同一个服务重复启动多个 watcher。
    private final Map<String, Object> watcherLocks = new ConcurrentHashMap<>();

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
        this.client = CuratorFrameworkFactory.builder()// 按照你提供的连接地址、超时配置、重试策略，创建一个 Curator 客户端对象， 创建一个 Java 里的 ZooKeeper 客户端，以后用它和 ZooKeeper 通信
                .connectString(finalConnectString)// ZooKeeper 地址是 connectString
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
            //  临时节点意味着：
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
        // Day6 的 discover 分成两步：
        // 1. 先看 JVM 本地缓存里有没有；
        // 2. 如果还没有，再去 ZooKeeper 拉一次初始数据，并启动 watcher。
        // 你可以把它想成：
        // - 第一次 discover：去“总通讯录”抄一份到自己本地
        // - 后面再 discover：先翻自己手里的本地通讯录
        List<ServiceInstance> cachedInstances = serviceCache.get(serviceName);
        if (cachedInstances != null) {
            // 返回副本，避免调用方误改我们内部缓存。
            // 例如外部拿到列表后做 clear()/remove()，都不应该影响注册中心内部真正那份缓存。
            return new ArrayList<>(cachedInstances);
        }

        Object watcherLock = watcherLocks.computeIfAbsent(serviceName, key -> new Object());// - 去 watcherLocks 里找这个 serviceName 对应的锁对象; 如果已经有了，就直接返回; 如果没有，就创建一个新的 Object() 放进去再返回
        synchronized (watcherLock) {// 同一个服务第一次 discover 时，只允许一个线程进入初始化逻辑
            cachedInstances = serviceCache.get(serviceName);
            if (cachedInstances != null) {// 这里要再检查一次缓存，是经典的“二次确认”。防止别的线程已经初始化过
                return new ArrayList<>(cachedInstances);// 原因是：多个线程可能同时第一次 discover 同一个服务。其中一个线程已经完成了初始化，后来的线程就不必再重复拉 ZooKeeper、重复建 watcher。
            }
            try {
                // （第一次）先拉一份当前最新实例列表到本地缓存。
                refreshServiceCache(serviceName);// 这一句会真的去 ZooKeeper 读子节点
                startServiceWatcherIfAbsent(serviceName); // 再启动 watcher，让后续的节点变化能持续同步到这份缓存。
                return new ArrayList<>(serviceCache.getOrDefault(serviceName, List.of()));// 最后把缓存副本返回给调用方
            } catch (Exception e) {
                throw new RuntimeException("从 ZooKeeper 发现服务实例失败: " + serviceName, e);
            }
        }
    }

    @Override
    public void close() {
        // 关闭 Curator 客户端，相当于断开和 ZooKeeper 的会话。
        // 如果当前客户端创建过临时节点，这些节点也会随会话结束而被删除。
        for (CuratorCache watcher : serviceWatchers.values()) {
            watcher.close();
        }
        serviceWatchers.clear();
        watcherLocks.clear();
        serviceCache.clear();
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

    private void startServiceWatcherIfAbsent(String serviceName) {// 给这个服务装一个后台监听器
        if (serviceWatchers.containsKey(serviceName)) {// 检查目前我们有没有给"serviceName"创建watcher监听
            return;
        }

        String watchedServicePath = servicePath(serviceName);
        // CuratorCache 可以理解成：
        // - 一边监听指定路径
        // - 一边在本地维护这个路径相关节点的缓存视图
        CuratorCache watcher = CuratorCache.build(client, watchedServicePath);// 那这个 watcher 是：“我这次新造的那个监听器”
        watcher.listenable().addListener(CuratorCacheListener.builder()// 让后面的变化可以自动同步
                // 第一次把缓存预热完成后，主动再刷一次，确保本地列表是最新的。
                .forInitialized(() -> refreshServiceCacheQuietly(serviceName))// watcher 初始化完成时执行什么
                // 只要这个服务下面有实例新增、实例地址变化，立刻更新本地缓存。
                .forCreatesAndChanges((oldNode, node) -> refreshServiceCacheQuietly(serviceName))// 节点新增或变化时执行什么
                // 实例下线、节点删除时，也要同步把缓存里的旧地址删掉。
                .forDeletes(node -> refreshServiceCacheQuietly(serviceName))// 节点删除时执行什么
                .build());
        watcher.start();// 启动监听

        CuratorCache existingWatcher = serviceWatchers.putIfAbsent(serviceName, watcher);// 如果这个 key 还没有值，就放进去; 如果这个 key 已经有值了，就不要放
        if (existingWatcher != null) {// 这个服务名原来已经有 watcher 了, 当前这个 watcher 没有被真正放进去, 它是一个“重复创建出来但没被采用”的 watcher
            watcher.close();
        }
    }

    private void refreshServiceCacheQuietly(String serviceName) {
        try {
            refreshServiceCache(serviceName);
        } catch (Exception e) {
            throw new RuntimeException("刷新 ZooKeeper 本地服务缓存失败: " + serviceName, e);
        }
    }

    private void refreshServiceCache(String serviceName) throws Exception {
        String fullServicePath = servicePath(serviceName);// 拼出服务路径
        if (client.checkExists().forPath(fullServicePath) == null) { // 这个服务当前还没有任何实例时，也把空列表缓存下来。
            // 这样客户端下一次 discover 至少先走本地缓存，不会重复打 ZooKeeper。
            // 注意：缓存成空列表，不等于“以后永远没有实例”。
            // 因为 watcher 仍然在，一旦后面有新实例注册进来，本地缓存会被刷新成非空列表。
            serviceCache.put(serviceName, List.of());
            return;
        }
        // ZooKeeper 里存的是一组子节点名，例如：
        // /mini-rpc/services/com.xxx.HelloService/127.0.0.1:9000
        List<String> children = client.getChildren().forPath(fullServicePath);// 读取我们拼出的服务路径下的全部子节点
        List<ServiceInstance> instances = new ArrayList<>();
        for (String child : children) {// 重新从 ZooKeeper 拉一遍完整实例列表
            ServiceInstance instance = parseInstance(serviceName, child);// 把子节点名还原成 ServiceInstance
            if (instance != null) {
                instances.add(instance);
            }
        }
        // 这里不是在旧列表上 add/remove，而是直接整体替换成一份最新快照。
        // 好处是：
        // 1. 逻辑简单，不容易漏边界情况
        // 2. watcher 收到事件后，不需要自己推演“到底应该加谁删谁”
        // 3. 当前列表永远以 ZooKeeper 此刻的真实结果为准
        //
        // List.copyOf(...) 还顺手把它变成不可变列表，避免内部缓存被意外修改。
        serviceCache.put(serviceName, List.copyOf(instances));// 整体覆盖旧的 serviceCache
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
        try {
            String host = childNodeName.substring(0, separatorIndex);
            int port = Integer.parseInt(childNodeName.substring(separatorIndex + 1));
            return new ServiceInstance(serviceName, host, port);
        } catch (NumberFormatException ignored) {
            // 如果节点名不是 host:port 这种格式，就说明这个节点不符合当前项目约定，直接跳过。
            return null;
        }
    }
}
