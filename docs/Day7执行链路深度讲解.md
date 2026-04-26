# Day7 执行链路深度讲解（序列化抽象 + 轮询负载均衡 + 异常分类）

## 1. Day7 要解决什么问题

到 Day6 为止，项目已经具备：

- RPC 基本通信链路
- ZooKeeper 注册与发现
- 客户端本地缓存 + watcher 自动刷新
- 多实例随机负载均衡
- 最小失败重试

但这时还有三块明显可以继续工程化的地方：

- **序列化方式写死在 `RpcMessageCodec`**
- **负载均衡只有随机策略**
- **客户端异常虽然能抛出，但失败来源不够清晰**

所以 Day7 的目标是：

- 把序列化抽成接口，并新增 JSON 方案
- 把负载均衡从“只有随机”升级成“随机 + 轮询可切换”
- 把调用失败细分成更明确的异常类型
- 同步补上更像工程代码的测试与轻量验证入口

一句话概括：

`Day7 不是再加一个新中间件，而是把前面已经跑通的链路进一步做成“可扩展、可切换、可验证”的小框架。`

## 2. Day7 的三个核心变化

### 2.1 序列化抽象

Day6 之前，`RpcMessageCodec` 里是写死的 JDK 原生序列化。

Day7 后，结构变成：

- `Serializer`：统一抽象
- `JdkSerializer`：默认实现
- `JsonSerializer`：新增 JSON 实现
- `SerializerFactory`：按 JVM 参数选择具体实现

所以现在客户端和服务端真正依赖的不是某个具体序列化类，而是：

`当前配置出来的 Serializer`

### 2.2 轮询负载均衡

Day6 之前，负载均衡只有：

- `RandomLoadBalancer`

Day7 后，新增：

- `RoundRobinLoadBalancer`
- `LoadBalancerFactory`

客户端入口可以通过 JVM 参数切换：

```text
-Drpc.loadbalancer=random
-Drpc.loadbalancer=round_robin
```

这让负载均衡从“写死一种策略”，升级成“按抽象支持多策略扩展”。

### 2.3 异常分类与重试语义

Day6 之前，很多调用失败最终还是落成：

- `RuntimeException("远程调用失败")`

Day7 后，客户端主要区分这些异常：

- `ServiceDiscoveryException`
- `RpcConnectionException`
- `RpcTimeoutException`
- `RemoteInvocationException`
- `RpcClientException`

同时把“哪些错误适合重试”也更明确地落到了代码里：

- 连接失败：适合有限重试
- 超时：适合有限重试
- 服务发现失败：通常不值得盲目重试
- 远端业务异常：通常不应该重试

## 3. Day7 的新结构分别放在哪

### 3.1 序列化相关

- [Serializer.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-core/src/main/java/com/minirpc/core/codec/serialization/Serializer.java)
- [JdkSerializer.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-core/src/main/java/com/minirpc/core/codec/serialization/JdkSerializer.java)
- [JsonSerializer.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-core/src/main/java/com/minirpc/core/codec/serialization/JsonSerializer.java)
- [SerializerFactory.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-core/src/main/java/com/minirpc/core/codec/serialization/SerializerFactory.java)
- [RpcMessageCodec.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-core/src/main/java/com/minirpc/core/codec/RpcMessageCodec.java)

### 3.2 负载均衡相关

- [LoadBalancer.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-registry/src/main/java/com/minirpc/registry/LoadBalancer.java)
- [RandomLoadBalancer.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-registry/src/main/java/com/minirpc/registry/RandomLoadBalancer.java)
- [RoundRobinLoadBalancer.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-registry/src/main/java/com/minirpc/registry/RoundRobinLoadBalancer.java)
- [LoadBalancerFactory.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-registry/src/main/java/com/minirpc/registry/LoadBalancerFactory.java)

### 3.3 异常分类相关

- [RpcClient.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-client/src/main/java/com/minirpc/client/RpcClient.java)
- [RpcClientProxy.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-client/src/main/java/com/minirpc/client/RpcClientProxy.java)
- [RpcClientException.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-client/src/main/java/com/minirpc/client/exception/RpcClientException.java)
- [ServiceDiscoveryException.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-client/src/main/java/com/minirpc/client/exception/ServiceDiscoveryException.java)
- [RpcConnectionException.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-client/src/main/java/com/minirpc/client/exception/RpcConnectionException.java)
- [RpcTimeoutException.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-client/src/main/java/com/minirpc/client/exception/RpcTimeoutException.java)
- [RemoteInvocationException.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-client/src/main/java/com/minirpc/client/exception/RemoteInvocationException.java)

## 4. Day7 之后一次请求的主链路有什么变化

### 4.1 发请求前

客户端启动后，仍然是：

- 动态代理组装 `RpcRequest`
- `RpcClient` 调 `discover(serviceName)`
- 注册中心返回实例列表
- 负载均衡器从多个实例里选一个目标实例

但这里多了一层“策略可切换”：

- `random`
- `round_robin`

也就是说，选实例这一步不再只有随机一种写死逻辑。

### 4.2 编码时

以前：

- `RpcMessageCodec` 里直接写死 JDK 序列化

现在：

1. `RpcMessageEncoder` 仍然调用 `RpcMessageCodec.encode(msg)`
2. `RpcMessageCodec` 通过 `SerializerFactory.getConfiguredSerializer()` 选择当前序列化实现
3. 当前实现可能是：
   - `JdkSerializer`
   - `JsonSerializer`
4. 对象再被转成字节数组发到网络

所以 Day7 的关键不是把编码器重写了，而是：

`编码器背后的序列化实现不再写死`

### 4.3 解码时

以前：

- `RpcMessageCodec.decode(bytes)` 直接用 JDK 反序列化

现在：

1. `RpcMessageDecoder` 仍然读取完整消息字节
2. `RpcMessageCodec.decode(bytes)` 通过当前 `Serializer` 反序列化
3. 还原出 `RpcRequest` 或 `RpcResponse`

所以客户端和服务端只依赖统一抽象，不依赖某一种具体序列化方案。

### 4.4 调用失败时

以前：

- 很多失败最后长得都像“远程调用失败”

现在：

- 服务没发现：`ServiceDiscoveryException`
- 连接失败：`RpcConnectionException`
- 等响应超时：`RpcTimeoutException`
- 服务端业务错误：`RemoteInvocationException`

这样做的好处是：

- 错误来源更清楚
- 重试判断更清楚
- 面试表达也更清楚

## 5. 为什么 Day7 选择 JSON，而不是先上 Kryo

Day7 这里的目标不是追极致性能，而是先把：

- 序列化可切换
- 抽象设计成立
- 端到端可验证

这三件事做稳。

JSON 的优点是：

- 可读性强
- 调试友好
- 讲解成本低
- 更适合当前这个学习型框架项目

所以 Day7 先选 JSON，非常合理。

后续如果还想继续扩展 Kryo，那已经有 `Serializer` 这个抽象基础了。

## 6. 为什么 Day7 的异常分类很重要

因为 RPC 失败不是一种失败。

最少会有这几类：

- 服务根本没发现到
- 连接目标实例失败
- 请求发出去了但迟迟没回来
- 请求回来了，但服务端业务执行报错

如果这些都统一写成：

`RuntimeException("远程调用失败")`

那项目虽然还能跑，但工程表达会很弱。

Day7 的价值就在于：

`让不同失败场景带着自己的语义返回`

这样以后继续扩展：

- 更细的重试策略
- 更细的监控统计
- 更细的日志归类

都会更自然。

## 7. Day7 的测试是怎么设计的

Day7 这次不是“功能做完最后再补测试”，而是跟着功能同步补。

测试分成了三层：

### 7.1 序列化 round-trip 测试

文件：

- [SerializerTest.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-core/src/test/java/com/minirpc/core/codec/serialization/SerializerTest.java)

验证：

- JDK 序列化能否正确 round-trip `RpcRequest`
- JSON 序列化能否正确 round-trip `RpcRequest`
- JSON 序列化能否正确 round-trip `RpcResponse`
- `SerializerFactory` 能否按配置创建正确实现

### 7.2 轮询策略测试

文件：

- [RoundRobinLoadBalancerTest.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-registry/src/test/java/com/minirpc/registry/RoundRobinLoadBalancerTest.java)

验证：

- 多实例下是否按轮询顺序选择
- 存在已排除实例时是否正确跳过
- `LoadBalancerFactory` 能否按配置创建正确策略

### 7.3 Day7 集成测试

文件：

- [Day7RpcIntegrationTest.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-demo/src/test/java/com/minirpc/demo/Day7RpcIntegrationTest.java)

验证：

- JSON 序列化放回完整 RPC 链路后仍能正常调用
- 轮询负载均衡在多实例场景下确实按顺序分发
- 服务未发现时抛 `ServiceDiscoveryException`
- 连接不可达时抛 `RpcConnectionException`
- 响应过慢时抛 `RpcTimeoutException`
- 服务端业务失败时抛 `RemoteInvocationException`

## 8. 收官轻量验证入口做了什么

Day7 还顺手把 [ClientLoadBootstrap.java](file:///d:/ProgramFiles/CodeProjects/mini-RPC/rpc-demo/src/main/java/com/minirpc/demo/ClientLoadBootstrap.java) 做成了更适合收官验证的入口。

现在它支持通过 JVM 参数调整：

- `rpc.load.threadCount`
- `rpc.load.requestsPerThread`
- `rpc.loadbalancer`
- `rpc.serializer`

并在启动时输出当前验证配置。

这样做的意义不是为了追求“大而重的正式压测”，而是为了提供一个：

- 可重复
- 可对比
- 能展示工程化思维

的轻量验证入口。

## 9. Day7 最值得记住的几句话

- `Day7 不是重写通信链路，而是把原有链路抽象成更像框架的结构`
- `Serializer 抽象解决的是“对象如何编码/解码可切换”`
- `RoundRobinLoadBalancer` 解决的是“负载均衡不再只有随机一种策略”`
- `异常分类解决的是“调用失败不再只是一个模糊的 RuntimeException”`
- `Day7 的测试不是补作业，而是用来证明策略切换、抽象能力和错误语义真的成立`

## 10. Day7 高价值问题与结论

### 10.1 Day7 的真正核心是什么

不是“加了 JSON”

也不是“多了个轮询类”

而是：

`把项目从单实现、单策略、弱语义，推进成多策略、可切换、可验证的小框架结构`

### 10.2 为什么说序列化抽象是框架化升级

因为 Day6 之前，编解码层只能绑定某一种具体实现。

Day7 之后：

- 编解码器依赖的是统一抽象
- 具体实现可以按配置切换

这就从“某个功能能跑”，升级成了“某类能力可扩展”。

### 10.3 为什么负载均衡要做成接口 + 多实现

因为负载均衡天然就是策略问题。

随机、轮询、本地优先、加权、最小连接数，这些本来就是不同策略。

所以：

- `LoadBalancer` 做抽象
- `RandomLoadBalancer` / `RoundRobinLoadBalancer` 做实现

这就是最符合扩展方向的设计。

### 10.4 为什么远端业务异常不应该和连接失败混在一起

因为两者的含义完全不同：

- 连接失败：网络层、实例层问题，适合换实例或重试
- 远端业务异常：说明请求已经送达并执行了，只是业务本身报错，这时盲目重试通常没有意义

这就是 Day7 里“异常分类 + 重试语义”要一起做的原因。

### 10.5 Day7 的测试为什么很有价值

因为它们不是在重复实现代码，而是在验证：

- 抽象是否真的成立
- 策略切换是否真的生效
- 异常分类是否真的准确
- 端到端链路是否在新设计下依然跑通

这类测试特别适合在面试时展示工程化思维。
