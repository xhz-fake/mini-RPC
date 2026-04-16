# Day5 执行链路深度讲解（ZooKeeper 注册中心落地）

## 1. Day5 要解决什么问题

Day4 已经把“服务注册与发现”这件事跑通了，但底层实现还是 `FileRegistryCenter`。  
它适合教学，却不适合真实分布式场景，因为：

- 服务实例信息只是写在本地文件里
- 多台机器之间没有统一的中心节点
- 服务端异常退出时，需要依赖手动注销或进程正常关闭

Day5 的目标就是：

- 保留 Day4 已经抽象好的 `RegistryCenter` 接口
- 把底层实现从 `FileRegistryCenter` 替换成 `ZookeeperRegistryCenter`
- 让服务端真正把实例注册到 ZooKeeper
- 让客户端真正从 ZooKeeper 发现实例

一句话概括：

`Day4 解决“机制是什么”，Day5 解决“真实注册中心怎么接进来”`

## 2. Day5 新增了什么

- 新增 `ZookeeperRegistryCenter`
- 为 `rpc-registry` 模块引入 Curator 客户端依赖
- `RegistryCenter` 增加 `close()` 生命周期方法，便于关闭底层 ZooKeeper 客户端
- `ServerBootstrap` / `ClientBootstrap` / `ClientLoadBootstrap` 改为使用 ZooKeeper 注册中心

## 3. Day5 和 Day4 的关键区别

Day4：

- 服务端注册时：往本地文件写 `serviceName=host:port`
- 客户端发现时：从本地文件读 `serviceName=host:port`

Day5：

- 服务端注册时：在 ZooKeeper 里创建节点
- 客户端发现时：从 ZooKeeper 里读取子节点

所以变化的核心不是 `RpcClient` 和 `RpcServer` 的主流程，而是：

`注册中心的数据存储位置，从文件变成了 ZooKeeper`

## 4. ZooKeeper 中的数据结构长什么样

Day5 默认把所有服务都挂在：

`/mini-rpc/services`

下面。

例如 `HelloService` 的节点结构是：

```text
/mini-rpc/services
  /com.minirpc.demo.service.HelloService
    /127.0.0.1:9000
```

含义是：

- `/mini-rpc/services`：项目根节点
- `/com.minirpc.demo.service.HelloService`：某个服务名对应的目录
- `/127.0.0.1:9000`：具体实例节点

## 5. 服务端启动后发生了什么

服务端入口还是 `ServerBootstrap`，但现在注册中心变成了 `ZookeeperRegistryCenter`。

执行过程是：

1. 服务端读取 `host` 和 `port`
2. 本地 `ServiceRegistry` 继续注册 `HelloService -> HelloServiceImpl`
3. 构造 `ServiceInstance(serviceName, host, port)`
4. 调用 `registryCenter.register(instance)`
5. `ZookeeperRegistryCenter` 连接 ZooKeeper
6. 确保根路径和服务路径存在
7. 创建实例节点 `/mini-rpc/services/服务名/host:port`

### 5.1 为什么实例节点使用临时节点

Day5 使用的是 ZooKeeper 的 `EPHEMERAL` 临时节点。

这意味着：

- **只要服务端进程还活着、ZooKeeper 会话还在，节点就存在**
- 如果服务端**异常退出、会话断开，节点会自动消失**

这就是 Day5 比 Day4 更接近真实注册中心的地方：

`不用完全依赖手动注销，注册中心自己就能感知实例是否还活着`

## 6. 客户端调用前发生了什么

客户端入口 `ClientBootstrap` 依然是：

- 创建 `RpcClient`
- 创建动态代理
- 调用 `helloService.hello("mini-RPC-1")`

但关键区别是：`RpcClient` 内部不再通过 `FileRegistryCenter.discover(...)` 读文件，
而是通过 `ZookeeperRegistryCenter.discover(...)` 读 ZooKeeper。

执行过程是：

1. 动态代理组装 `RpcRequest`
2. `RpcClient#selectServiceInstance(...)` 调用 `registryCenter.discover(serviceName)`
3. `ZookeeperRegistryCenter` 去 ZooKeeper 读**服务节点下**的**全部子节点**
4. 把子节点名 `127.0.0.1:9000` 还原成 `ServiceInstance`
5. 返回 `List<ServiceInstance>`
6. 再由 `RandomLoadBalancer` 选出一个目标实例

## 7. ZooKeeper 发现实例时，代码到底在干什么

Day5 的 `discover(serviceName)` 可以翻译成一句人话：

`去 ZooKeeper 问：这个服务名下面目前挂着哪些实例节点？`

它会做这些事：

1. 拼出服务路径，例如：
   `/mini-rpc/services/com.minirpc.demo.service.HelloService`
2. 判断这个路径是否存在
3. 读取该路径下所有子节点名
4. 逐个把子节点名解析成 `ServiceInstance`
5. 返回给 `RpcClient`

这里最关键的一点是：

`客户端拿到的依然是 List<ServiceInstance>`

所以对 `RpcClient` 来说：

- Day4 返回 `List<ServiceInstance>`
- Day5 也返回 `List<ServiceInstance>`

这说明 Day4 抽象出来的 `RegistryCenter` 接口起作用了。

## 8. 为什么说 Day5 尽量没动主链路

Day5 仍然保留了这些核心逻辑：

- 动态代理组装 `RpcRequest`
- `RpcClient` 发送请求
- `requestId -> future` 精准配对
- `endpoint -> channel` 连接缓存
- `RandomLoadBalancer` 选实例
- 服务端反射调用真实业务方法

也就是说，Day5 不是推翻 Day4 重写一遍，而是：

`把“从哪发现实例”这一层底座换掉了`

这正是**面向接口编程**的价值。

## 9. Day5 最重要的新增理解点

### 9.1 注册中心从“文件”变成了“外部中间件”

Day4：

- `registryFile` 是一个本地文件路径

Day5：

- ZooKeeper 是一个独立服务
- Java 项目通过 Curator 客户端连接它

所以 Day5 你要多理解一个新概念：

`注册中心不再和项目进程绑死，而是变成了进程外部的独立组件`

### 9.2 注册动作变成“创建节点”

Day4：

- 把地址写进 `properties` 文件

Day5：

- **在 ZooKeeper 中创建 ZNode**

### 9.3 发现动作变成“读子节点”

Day4：

- 从文件里按 `key=value` 读取

Day5：

- 从 ZooKeeper 路径下**读取 children**

## 10. Day5 调试建议

建议优先关注这些位置：

1. `ZookeeperRegistryCenter` 构造方法：是否成功连接 ZooKeeper
2. `register(instance)`：服务端是否成功创建实例节点
3. `discover(serviceName)`：客户端是否成功读到子节点
4. `RandomLoadBalancer#select(...)`：最终选中了哪个实例
5. `RpcClient#ensureConnected(instance)`：是否对目标实例完成建连/复用
6. `RpcClientResponseHandler#channelRead0(...)`：响应是否正确回填

## 11. Day5 高价值问题与易混点

这一节专门收口 Day5 最容易混淆、但最值得记住的高价值问题。

### 11.1 Day5 的真正核心变化到底是什么

最容易说错的一句话是：

`服务迁移到了 ZooKeeper 上`

这其实不准确。更准确的说法是：

`服务实例地址的注册与发现，从本地文件迁移到了 ZooKeeper 注册中心`

也就是说：

- 真正提供业务能力的，仍然是 `HelloServiceImpl`
- 真正监听端口、处理请求的，仍然是 `RpcServer`
- 真正发请求的，仍然是 `RpcClient`
- 变化的只是“客户端如何知道服务端在哪”

### 11.2 为什么 Day5 不是重写，而是“换底座”

Day5 看起来引入了 ZooKeeper，但实际上主链路没有推翻重来。

保留下来的能力有：

- 动态代理组装 `RpcRequest`
- `RpcClient` 发送请求
- `requestId -> future` 精准配对
- `endpoint -> channel` 连接缓存
- 负载均衡选实例
- 服务端反射调用真实业务方法

变化的是：

- Day4 用 `FileRegistryCenter`
- Day5 用 `ZookeeperRegistryCenter`

所以 Day5 的本质是：

`把注册中心底座从文件实现，升级成了 ZooKeeper 实现`

### 11.3 为什么实例节点用 `EPHEMERAL` 临时节点

Day5 使用 ZooKeeper 临时节点注册实例，最核心的价值是：

- **服务端活着，节点就在**
- **服务端异常退出、会话断开，节点会自动消失**

这说明 Day5 的注册中心已经具备“自动感知实例存活状态”的能力。  
这也是 Day5 比 Day4 更接近真实注册中心的重要原因。

### 11.4 既然临时节点会自动删除，为什么还要 `unregister(instance)`

严格从“功能是否能工作”来说，`unregister(instance)` 不是绝对必须。  
因为就算服务端来不及主动清理，只要 ZooKeeper 会话断掉，临时节点最终还是会自动消失。

但是工程上仍然建议保留 `unregister(instance)`，因为它负责：

- **正常关闭时主动、立即地下线实例**
- 减少客户端误选“刚要下线但还没超时清除”的实例窗口
- 让“主动下线”和“异常掉线”在语义上更清晰

可以把两者理解成：

- `unregister`：主动优雅下线
- `EPHEMERAL`：异常场景自动兜底

### 11.5 为什么服务端同时写了 `shutdownHook` 和 `finally`

这两层确实有重叠，但不是无意义重复。

- `finally`
  - 偏“当前代码流程结束时的收尾”
  - 例如 `rpcServer.start()` 异常退出后，当前 `main` 方法结束时执行

- `shutdownHook`
  - 偏“JVM 退出时的兜底善后”
  - 例如你在 IDEA 里点停止，JVM 退出前尽量执行一次清理逻辑

所以它们属于两层保障：

- `finally`：代码块级别善后
- `shutdownHook`：进程级别善后

### 11.6 为什么客户端没有 `unregister(instance)`

因为客户端和服务端在 ZooKeeper 里的角色不一样。

服务端是：

- 服务提供者
- 会把自己的实例节点注册到 ZooKeeper

客户端是：

- 服务消费者
- 只负责查询服务有哪些实例
- 并不会把自己注册为某个服务实例

所以客户端关闭时只需要：

- `rpcClient.shutdown()`：关闭和服务端之间的 Netty 连接
- `registryCenter.close()`：关闭和 ZooKeeper 之间的 Curator 连接

而不需要：

- `unregister(instance)`

### 11.7 Day5 里到底有几类“连接”

这是理解 Day5 系统结构最关键的点之一。

Day5 至少有两类连接：

#### 第一类：RPC 连接

这是：

`客户端 <-> 服务端`

也就是：

- Netty 建立的 TCP 连接
- 用于真正传输 `RpcRequest` / `RpcResponse`

#### 第二类：ZooKeeper 连接

这是：

- `服务端 <-> ZooKeeper`
- `客户端 <-> ZooKeeper`

也就是：

- Curator 建立的 ZooKeeper 会话连接
- 服务端用它注册/注销实例
- 客户端用它发现实例

所以 Day5 的系统结构可以理解成：

`服务端 --(Curator/ZK连接)--> ZooKeeper`
`客户端 --(Curator/ZK连接)--> ZooKeeper`
`客户端 --(Netty/TCP连接)--> 服务端`

### 11.8 Day5 最值得带走的一句话

`Day5 不是改写 RPC 通信链路，而是把“服务实例地址如何注册、如何发现”的底座，从本地文件替换成了 ZooKeeper 中间件。`

## 12. 什么时候可以进入 Day6

不需要把 Day5 每一行代码都背下来，才有资格进入 Day6。  
真正的标准是：你是否已经抓住了 Day5 的主线与关键角色关系。

如果你已经能比较顺地讲出下面这些内容，就可以进入 Day6：

1. **Day5 和 Day4 的本质差异**
   - Day4：文件注册中心
   - Day5：ZooKeeper 注册中心

2. **服务端在 Day5 做了什么**
   - 本地注册业务实现
   - 连接 ZooKeeper
   - 注册实例节点
   - 启动 Netty 服务端

3. **客户端在 Day5 做了什么**
   - 连接 ZooKeeper
   - 发现实例列表
   - 负载均衡选实例
   - 建连、发请求、收响应

4. **ZooKeeper 节点结构长什么样**
   - `/mini-rpc/services/服务名/host:port`

5. **为什么要用临时节点**
   - 异常退出时自动删除实例节点

6. **Day5 里有哪些连接**
   - 客户端与服务端的 RPC 连接
   - 客户端与 ZooKeeper 的连接
   - 服务端与 ZooKeeper 的连接

7. **Day5 保留了哪些 Day4 能力**
   - requestId 精准配对
   - 连接复用
   - 负载均衡
   - Netty 通信主链路

如果这些点你已经大体讲得顺，而且启动流程也已经亲手跑通，那么就说明：

`Day5 已经过关，可以进入 Day6`

## 13. Day5 一句话总结

Day5 的本质，是把 Day4 的“教学版文件注册中心”升级成“真实中间件注册中心”。  
上层 `RpcClient` / `RpcServer` 的调用主链路基本保持不变，真正变化的是服务实例信息的存储与发现方式：  
服务端把实例注册到 ZooKeeper，客户端从 ZooKeeper 发现实例，再继续沿用 Day4 已经建立好的负载均衡、连接复用和请求响应配对机制。
