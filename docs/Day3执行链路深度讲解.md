# Day3 执行链路深度讲解（连接复用 + 超时 + 响应映射）

## 1. 本阶段目标

Day2 已经跑通 Netty 通信，但还有三个真实工程问题没有解决：

- 每次请求都**可能重新建连**，连接成本高
- 并发请求时，**响应无法稳定匹配**到正确调用方
- 异常或网络抖动时，请求**可能无限等待**

Day3 的核心目标就是解决这三件事。

## 2. 与 Day2 的变化对比

Day2：

- 单请求等待模型（CountDownLatch）
- 连接可用时正常返回，断连场景处理有限
- 一请求一连接的思路还未完全收敛

Day3：

- 改为 `requestId -> CompletableFuture<RpcResponse>` 映射
- 加入请求**超时控制**（5 秒）
- 客户端**连接复用** + 断连后**重连重试**
- 通道异常时批量失败回填 pending 请求

## 3. 功能实现顺序（从调用点深挖）

### 第 1 层：业务调用入口不变

调用仍然从 `helloService.hello("mini-RPC")` 发起。  
代理层依然组装 `RpcRequest` 并调用 `RpcClient#sendRequest`。

这说明 Day3 仍然只改通信治理，不改上层调用语义。

### 第 2 层：发送前先挂起“请求凭证”

进入 `sendRequest` 后，先做两件事：

1. 生成并保存一个 future（key 是 requestId）
2. 确保连接可用（连接复用或重连）

为什么先放映射再发请求：  
避免响应回得很快时，客户端还没来得及建立映射，导致响应丢失。

### 第 3 层：连接复用与重连

`ensureConnected` 的策略是：

- 如果 `channel` 活跃，直接复用
- 如果不可用，进入同步锁
- 在锁内做最多 3 次连接重试，每次失败后短暂退避
- 新连接建立后注册 closeFuture 回调，断连时把 channel 清空

这样后续请求会自动触发重新连接，不需要调用方感知。

### 第 4 层：请求发送与超时控制

请求发送成功后，调用方阻塞等待 future：

- 正常响应：future 被完成并返回 RpcResponse
- 超时：5 秒后抛出超时异常并清理映射
- 执行异常：抛出远程调用失败并清理映射

这个机制让“同步调用体验”继续成立，同时具备可控失败边界。

### 第 5 层：响应如何精准回到正确请求

客户端响应处理器收到 `RpcResponse` 后：

1. 用 `response.requestId` 找 pending future
2. 找到就 complete
3. 调用线程被唤醒，拿到自己的响应

这就是并发场景下“请求-响应配对”的核心。

### 第 6 层：异常链路如何兜底

当连接断开或处理器报错时：

- 遍历 pendingRequests
- 把所有未来结果标记为失败（completeExceptionally）
- 避免调用线程永久等待

这一步是 Day3 稳定性的关键补丁。

## 4. 新增观测入口（并发演示）

`ClientLoadBootstrap` 用于快速压测并观察通信指标：

- 多线程并发发起调用
- 输出总请求数、成功数、失败数、超时数
- 可用于验证连接复用与异常处理是否工作正常

这是 Day3 新增的“可观测入口”。

## 5. Day3 脑内模型

`本地方法调用`
→ `代理组装 RpcRequest`
→ `requestId 对应 future 入表`
→ `ensureConnected 复用/重连`
→ `writeAndFlush 出站发送`
→ `客户端等待 future（带超时）`
→ `服务端处理并回包`
→ `客户端 handler 按 requestId 完成 future`
→ `返回结果`

异常分支：

`断连/异常`
→ `failAllPending`
→ `调用方立即感知失败`

## 6. 你需要掌握的面试要点

- 为什么要 requestId -> Future 映射：支持并发请求精准配对
- 为什么要连接复用：减少连接建立开销，提升吞吐
- 为什么要超时：给调用方明确失败边界
- 为什么要批量失败回填：避免请求悬挂导致线程耗尽

## 7. 建议调试顺序

1. `RpcClient#sendRequest`：看 pendingRequests 何时 put/remove
2. `RpcClient#ensureConnected`：看连接复用与重连分支
3. `RpcClientResponseHandler#channelRead0`：看 requestId 命中 future
4. `RpcClientResponseHandler#channelInactive`：看 failAllPending

## 8. 实战调试记录（本次已验证）

### 8.1 连接复用验证结论

调试方式：

- 在同一个 `ClientBootstrap` 进程内连续发两次请求（`mini-RPC-1`、`mini-RPC-2`）
- 只保留两个断点：
  - `RpcClient.java` 连接判断 `if (localChannel != null && localChannel.isActive())`
  - `RpcClient.java` 连接赋值 `channel = newChannel`

实际观察到：

- 连接判断断点命中 2 次（对应两次请求）
- 连接赋值断点命中 1 次（只在第一次请求发生）
- 第二次请求时 `localChannel` 非空且活跃，直接复用连接

结论：

- Day3 连接复用生效，第二次请求未重复建连。

### 8.2 requestId 精准配对验证（下一步按此操作）

目标：确认“响应回来的时候，命中的是发起它的那一个 future”。

准备：

- 客户端 VM options 增加：`-Drpc.client.timeout.seconds=120`
- 保留以下断点：
  1. `RpcClient.java`：`pendingRequests.put(request.getRequestId(), future)`
  2. `RpcClientResponseHandler.java`：`pendingRequests.remove(msg.getRequestId())`
  3. `RpcClientResponseHandler.java`：`future.complete(msg)`
  4. `RpcClient.java`：`RpcResponse response = future.get(...)`

按钮操作：

- 全程主要按 `F9`（继续运行）
- 仅在需要执行当前行后看变量变化时按 `F8`

你应看到：

1. 在 put 断点处，`request.requestId` 有值  
2. 到 remove 断点时，`msg.requestId` 与上一步一致  
3. `future != null`，说明命中了正确请求  
4. 执行 `future.complete(msg)` 后，`future.get(...)` 那个线程被唤醒，拿到响应

这个现象证明：

- Day3 的 `requestId -> CompletableFuture` 映射配对机制正确工作，并发场景不会串响应。

### 8.3 Day3 验证完成清单（已完成）

本轮已完成的实战验证项如下：

- 连接复用验证通过  
  - 连接判断断点命中 2 次  
  - 连接赋值断点命中 1 次  
  - 第二次请求复用已有活跃连接

- requestId 精准配对验证通过  
  - 两次请求生成了不同 requestId：  
    - `54284a6a-3187-4200-989d-77a3081ffcd4`  
    - `620ce356-6b02-457a-8ea9-903594e8c021`  
  - 服务端均打印 `methodName:hello`  
  - 客户端最终输出：  
    - `hello mini-RPC-1`  
    - `hello mini-RPC-2`  
  - 说明两次响应都回到了各自对应的请求，没有串响应

- 调试稳定性优化已启用  
  - 客户端超时时间支持 JVM 参数覆盖：  
    `-Drpc.client.timeout.seconds=120`  
  - 用于避免断点停留过久触发误判超时

建议：  
完成以上清单后，可进入 Day3 统一复盘（调用链、并发配对、异常兜底、面试表达）。
