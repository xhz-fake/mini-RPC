# Day6 执行链路深度讲解（Watcher + 本地缓存）

## 1. Day6 要解决什么问题

Day5 已经把注册中心底座换成了 ZooKeeper。

这时客户端发现服务的大致方式是：

- 每次发请求前
- 都调用一次 `registryCenter.discover(serviceName)`
- 再去 ZooKeeper 现查当前实例列表

这个方案能跑通，但还有两个明显问题：

- **每次请求都查 ZooKeeper，开销偏大**
- **客户端虽然能拿到最新实例，但每次都要走一次“远程查询注册中心”**

所以 Day6 的目标是：

- 让客户端第一次发现服务时，先从 ZooKeeper 拉一份实例列表
- 把这份实例列表**缓存到**当前 JVM **内存中**
- 之后优先读本地缓存
- 同时通过 Curator 的 Watcher / CuratorCache 持续监听 ZooKeeper 节点变化
- 一旦服务实例上下线，就自动刷新本地缓存

一句话概括：

`Day5 是“现查现用”，Day6 是“先拉到本地，再靠 watcher 持续同步”`

## 2. Day6 的核心变化是什么

Day6 没有推翻 Day5 主链路。

这些东西都没有变：

- 动态代理照样组装 `RpcRequest`
- `RpcClient` 仍然调用 `registryCenter.discover(serviceName)`
- 负载均衡还是从 `List<ServiceInstance>` 里选一个实例
- Netty 通信、请求响应配对、连接复用都保持不变

真正变化的是：

- `ZookeeperRegistryCenter` 内部增加了**本地缓存**
- `ZookeeperRegistryCenter` 内部增加了 **CuratorCache 监听器**
- `discover(serviceName)` **不再默认“每次都直接读 ZooKeeper”**

所以 Day6 的本质是：

`把客户端的服务发现，从“直接查注册中心”，升级成“缓存优先 + watcher 自动刷新”`

## 3. Day6 新增了什么

- 为 `rpc-registry` 模块新增 `curator-recipes` 依赖
- `ZookeeperRegistryCenter` 增加 `serviceCache`
- `ZookeeperRegistryCenter` 增加 `serviceWatchers`
- `ZookeeperRegistryCenter` 增加 watcher 启动锁，避免重复监听同一个服务
- `discover(serviceName)` 改为缓存优先
- `close()` 关闭时**同时清理 watcher、本地缓存和 Curator 客户端**

## 4. 现在的 discover 到底在干什么

现在 `discover(serviceName)` 的执行逻辑可以翻译成下面这几句话：

### 4.1 第一次发现某个服务时

1. 先看 `serviceCache` 里有没有这个服务名对应的实例列表
2. 如果没有，说明这是第一次发现这个服务
3. 去 ZooKeeper 拉取一次当前实例列表
4. 把结果缓存到 `serviceCache`
5. 为这个服务启动一个 `CuratorCache` watcher
6. 返回这份实例列表给 `RpcClient`

### 4.2 第二次及以后再发现同一个服务时

1. 直接从 `serviceCache` 取出实例列表
2. 不再主动去 ZooKeeper 重新拉一遍
3. 把缓存结果交给负载均衡器

所以 Day6 后，客户端查服务的感觉变成了：

`第一次慢一点，因为要初始化缓存；后面快很多，因为直接读 JVM 本地内存`

## 5. 本地缓存长什么样

Day6 的本地缓存本质上就是：

```text
serviceName -> List<ServiceInstance>
```

例如：

```text
com.minirpc.demo.service.HelloService
  -> [127.0.0.1:9000, 127.0.0.1:9001]
```

注意这里缓存的不是：

- 不是 `HelloServiceImpl` 业务对象
- 不是 Netty `Channel`
- 不是 ZooKeeper 节点对象

而是：

`一组“这个服务当前有哪些实例地址”的说明书`

这点一定要抓住。

## 6. watcher 在监听什么

Day6 里，每个服务名都会对应一个 watcher。

比如 `HelloService` 的 watcher 盯住的是：

```text
/mini-rpc/services/com.minirpc.demo.service.HelloService
```

它关心的是这个路径下面的变化：

- 新增子节点
- 删除子节点
- 节点变化
- 缓存初始化完成

对我们这个项目来说，最重要的其实就是：

- **新增子节点**：说明有新实例上线
- **删除子节点**：说明有实例下线

一旦发生这些变化，watcher 就会重新刷新本地缓存。

## 7. 为什么 Day6 适合用 CuratorCache

因为它刚好解决了我们这里最想解决的三件事：

- **能在本地保留一份缓存快照**
- **能订阅 ZooKeeper 节点变化**
- **连接断开再恢复后，能自动重建缓存**

所以 Day6 选择它非常自然，不需要自己手写很多底层 watcher 细节。

## 8. Day6 运行时完整链路

下面按你最关心的“代码运行顺序”来串一次。

### 8.1 服务端启动

服务端主流程和 Day5 一样：

1. `ServerBootstrap` 启动
2. 本地 `ServiceRegistry` 注册 `HelloService -> HelloServiceImpl`
3. 创建 `ZookeeperRegistryCenter`
4. 调用 `register(instance)`
5. 在 ZooKeeper 创建：

```text
/mini-rpc/services/com.minirpc.demo.service.HelloService/127.0.0.1:9000
```

6. `RpcServer` 开始监听端口

这里和 Day5 的区别不大。

### 8.2 客户端第一次调用前

客户端主流程是：

1. `ClientBootstrap` 启动
2. 创建 `ZookeeperRegistryCenter`
3. 创建 `RpcClient`
4. 动态代理准备发起 `hello(...)`
5. `RpcClient#selectServiceInstance(...)`
6. 调用 `registryCenter.discover(serviceName)`

这时 Day6 的关键动作开始了：

1. 发现本地 `serviceCache` 还没有这个服务
2. 去 ZooKeeper 拉一次 `/mini-rpc/services/服务名` 下的子节点
3. 把子节点解析成 `List<ServiceInstance>`
4. 写入 `serviceCache`
5. 启动这个服务对应的 `CuratorCache` watcher
6. 返回实例列表给 `RpcClient`
7. 负载均衡器从实例列表里选一个
8. `RpcClient` 与目标服务端建立或复用连接
9. 发请求、收响应

### 8.3 客户端第二次调用前

再来一次 `hello(...)` 时：

1. 还是会走到 `registryCenter.discover(serviceName)`
2. 但这次 `serviceCache` 里已经有值了
3. 直接返回缓存里的实例列表
4. 不需要重新去 ZooKeeper 查一遍

所以第二次调用时，服务发现这一步已经更轻了。

## 9. 缓存什么时候会刷新

Day6 的刷新时机主要有两类：

### 9.1 第一次 discover 时刷新

这是“主动拉一次”的刷新。

作用是：

- 让缓存先有初始值
- 避免 watcher 启动前本地什么都没有

### 9.2 ZooKeeper 节点变化时刷新

这是“被 watcher 推动”的刷新。

典型场景：

- 新服务端启动并注册实例
- 老服务端关闭并注销实例
- 异常退出导致临时节点自动消失

这时 watcher 会监听到变化，然后重新拉取最新子节点列表，覆盖本地缓存。

## 10. 为什么不是 watcher 事件里自己手动增删一条缓存

这是个很值得记住的设计点。

理论上我们当然也可以这样做：

- 收到新增事件，就往缓存里 `add`
- 收到删除事件，就从缓存里 `remove`

但 Day6 没这么写，而是选择：

`只要发生变化，就重新拉一次完整实例列表，整体覆盖本地缓存`

这么做的好处是：

- 逻辑更简单
- 不容易因为事件顺序、边界场景而把缓存维护乱
- 更适合当前这个教学项目

这属于一个典型的取舍：

`牺牲一点点刷新时的性能，换来更简单、更稳、更容易理解的代码`

## 11. 为什么要加 watcher 启动锁

假设同一个客户端进程里，多个线程第一次同时调用 `HelloService`。

如果不加锁，可能出现：

- 线程 A 发现缓存没有值，准备启动 watcher
- 线程 B 也发现缓存没有值，也准备启动 watcher
- 最后同一个服务被重复监听多次

所以 Day6 增加了：

```text
serviceName -> lock
```

作用是：

- 同一个服务第一次初始化时，只允许一个线程真正去拉取缓存并启动 watcher
- 其他线程等它完成后直接复用结果

## 12. 为什么 close() 里要同时关闭 watcher

Day6 之后，`ZookeeperRegistryCenter` 不再只有一个 Curator 客户端资源。

它还持有：

- 多个 `CuratorCache` watcher
- 一份本地缓存

所以关闭时要做三件事：

1. 关闭全部 watcher
2. 清空 watcher 容器和本地缓存
3. 关闭 Curator 客户端

否则就可能出现：

- 监听线程没释放
- JVM 里还残留旧缓存
- 下次重新启动时行为不干净

## 13. Day6 和 Day5 的关键区别

Day5：

- discover 基本等于“实时查 ZooKeeper”

Day6：

- discover 基本等于“优先读 JVM 本地缓存”
- watcher 在后台持续保持缓存与 ZooKeeper 同步

所以两天最大的差异不是“注册中心换没换”，而是：

`客户端发现服务的读取模式变了`

## 14. 调试时建议重点看哪里

建议你后面复盘 Day6 时，优先盯住这些位置：

1. `ZookeeperRegistryCenter#discover(...)`
   看第一次是不是先走初始化缓存
2. `ZookeeperRegistryCenter#refreshServiceCache(...)`
   看 ZooKeeper 子节点是怎么变成 `List<ServiceInstance>` 的
3. `ZookeeperRegistryCenter#startServiceWatcherIfAbsent(...)`
   看 watcher 是什么时候启动的
4. `RpcClient#selectServiceInstance(...)`
   看 `RpcClient` 其实几乎没变，但服务发现结果已经换成缓存优先了
5. `ServerBootstrap` 的 `register(instance)` / `unregister(instance)`
   看服务端上下线如何触发缓存变化

## 15. Day6 最值得记住的几句话

- `Day6 不是换注册中心，而是升级服务发现读取模式`
- `客户端第一次拉 ZooKeeper，后面优先读 JVM 本地缓存`
- `watcher 的作用不是替客户端发请求，而是持续维护本地实例列表`
- `缓存里存的不是业务对象，而是服务实例地址说明书`
- `节点一变，不是手工修修补补，而是整体重新拉一份最新列表`

## 16. 现在你可以怎么理解 Day6

如果 Day5 你可以理解成：

`客户端每次打电话前，都先问一次通讯录管理员`

那 Day6 更像是：

`客户端先把通讯录抄到自己手里，同时管理员一旦发现通讯录有变化，就立刻通知你更新`

这就是 Day6 最核心、也最贴近真实工程场景的价值。

## 17. Day6 高价值问题与结论

这一节专门收口 Day6 最容易混淆、但面试前最值得反复看的问题。

### 17.1 Day6 的真正核心变化是什么

最容易说散的一种说法是：

`Day6 就是在 ZooKeeper 上加了个 watcher`

这不够准确。更准确的说法是：

`Day6 把客户端服务发现从“每次现查 ZooKeeper”，升级成了“缓存优先 + watcher 自动刷新”`

也就是说，watcher 不是目标本身，它只是为了维护客户端 JVM 本地缓存。

### 17.2 Day6 的本地缓存里到底存的是什么

缓存里存的不是：

- 不是 `HelloServiceImpl` 业务对象
- 不是 Netty `Channel`
- 不是 ZooKeeper 节点对象

缓存里存的是：

`serviceName -> List<ServiceInstance>`

也就是：

`某个服务当前有哪些可用实例地址`

这是 Day6 最根本的理解点。

### 17.3 为什么一个服务会有多个实例

因为“服务”是逻辑能力名，“实例”是这个能力当前落在哪个地址上的运行副本。

例如：

- `HelloService` 是服务
- `127.0.0.1:9000` 是一个实例
- `127.0.0.1:9001` 也是同一个服务的另一个实例

多个实例的意义是：

- 分摊流量
- 提升可用性
- 支持负载均衡
- 某个实例失败时，客户端还有机会切换到其他实例

### 17.4 为什么 `discover(serviceName)` 返回的是实例列表，而不是实现类对象

因为客户端真正需要知道的不是：

- `HelloServiceImpl` 这段代码长什么样

而是：

- `HelloService` 这个服务现在可以发往哪些 `host:port`

RPC 客户端最终只能把请求发给某个网络地址，所以注册中心返回的一定是实例地址列表，而不是服务端 JVM 里的实现类对象。

### 17.5 为什么 `discover(...)` 要先返回缓存副本，而不是直接返回内部缓存

如果把内部缓存原样返回，外部代码拿到列表后做：

- `clear()`
- `remove()`
- `add()`

就可能直接把注册中心自己维护的缓存改坏。

所以 Day6 这里返回的是：

`new ArrayList<>(cachedInstances)`

意思是：

`给调用方一份副本，但别碰内部那份真缓存`

### 17.6 为什么 watcher 收到事件后，不是自己在旧缓存上 add/remove，而是整体重刷

Day6 这里做的是：

`节点一变 -> 重新从 ZooKeeper 拉完整实例列表 -> 整体覆盖本地缓存`

这样做的好处是：

- 逻辑更简单
- 不容易被事件顺序和边界情况搞乱
- 本地缓存始终以 ZooKeeper 当前真实状态为准

这是一个典型的工程取舍：

`牺牲一点点刷新性能，换来更稳、更容易理解的缓存一致性维护方式`

### 17.7 为什么同一个服务已经有 watcher 了，还要 `watcher.close()`

这里最容易误会的点是：

`关闭的不是原来已经在 Map 里的正式 watcher`

而是：

`当前新建出来、但因为别的线程已经先注册成功而变成多余副本的 watcher`

也就是说：

- 正式 watcher 保留
- 重复新建出来但没用上的 watcher 关闭

这样做是为了避免：

- 重复监听
- 资源浪费
- 多个 watcher 同时处理同一服务路径事件

### 17.8 为什么这里要用接口变量调用 `discover(...)`

例如：

```java
RegistryCenter registryCenter = new ZookeeperRegistryCenter();
registryCenter.discover(serviceName);
```

这里不是说“接口自己有对象”，而是：

- 左边 `RegistryCenter` 是接口类型变量
- 右边 `new ZookeeperRegistryCenter()` 是实际创建出来的实现类对象

这样写体现的是：

- 面向接口编程
- 多态
- 上层依赖抽象，不依赖具体实现

好处是以后底层从：

- `FileRegistryCenter`

切到：

- `ZookeeperRegistryCenter`

甚至以后切到其他实现时，上层调用代码几乎不用改。

### 17.9 JVM 是怎么知道该调用哪个 `discover()` 实现的

编译时只检查：

- `RegistryCenter` 接口里有没有 `discover(...)`

运行时真正决定调用哪个实现的，是变量里实际引用的对象类型。

例如：

```java
RegistryCenter registryCenter = new ZookeeperRegistryCenter();
```

那运行时调用的就是：

- `ZookeeperRegistryCenter#discover(...)`

如果换成：

```java
RegistryCenter registryCenter = new FileRegistryCenter();
```

那运行时调用的就会变成：

- `FileRegistryCenter#discover(...)`

所以最短总结是：

`编译时看接口，运行时看实际对象`

### 17.10 Day6 结束时，项目能力相比 Day5 提升了什么

Day5 已经具备：

- ZooKeeper 注册与发现

Day6 在此基础上再往前走了一步：

- 客户端不再每次都直接打注册中心
- 引入 JVM 本地缓存降低重复查询开销
- 借助 watcher 自动感知实例上下线
- 让客户端具备“更接近真实服务治理”的动态服务感知能力

所以 Day6 的价值，不只是“多了一个 watcher API”，而是：

`服务发现从被动查询，升级成了缓存化、动态化、持续同步的模型`
