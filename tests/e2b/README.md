# E2B HTTP 测试

用 Python 通过真实 HTTP
驱动沙箱，验证跨请求状态保持、隔离性、TypeScript、超时恢复与环境变量作用域。

## 两个 Python 套件的分工

| 文件                 | 被测对象                              | 依赖                               | 启动方式                                                               |
| -------------------- | ------------------------------------- | ---------------------------------- | ---------------------------------------------------------------------- |
| `test_executor.py`   | **内层执行器**（`/internal/execute`） | 仅标准库                           | 以 `e2b-executor-harness` 作为 main service，需在 `crates/base` 下启动 |
| `test_sdk.py`        | **公开 E2B 适配器 API**               | `pip install e2b-code-interpreter` | 以 `examples/e2b-adapter` 作为 main service                            |
| `bench_sandboxes.py` | **沙箱创建速度**（不是功能）          | `pip install e2b`                  | 同 `test_sdk.py`                                                       |

`test_sdk.py` 用**官方 E2B Code Interpreter Python SDK** 的同步和异步客户端
驱动生命周期与 `run_code`，所以字段名、状态码、令牌或 NDJSON 事件一旦和官方
客户端对不上，这里就会失败，而不是悄悄溜过去。

适配器套件的启动方式（仓库根目录）：

```bash
cargo build
pip install e2b-code-interpreter
E2B_API_KEY=e2b_0000000000000000000000000000000000000000 \
./target/debug/edge-runtime start --main-service ./examples/e2b-adapter -p 9998
```

`EXECUTOR_SERVICE_PATH` 默认是仓库根目录相对路径
`./examples/e2b-executor`，所以上面的命令不需要覆盖它。从其他工作目录启动时才需要
显式设置。

另一个终端：

```bash
python3 tests/e2b/test_sdk.py
```

本分支实测 `21/21 passed`，使用 `e2b-code-interpreter 2.8.1` 和
`e2b 2.46.0`。套件默认用上面那个 key，只要和启动适配器时用的一致就行；
不一致时脚本会直接提示，而不是报出一堆 401
失败。适配器对生命周期/控制平面和自定义 `/execute` 路由**失败即拒绝**：不设
`E2B_API_KEY` 时这些请求返回 401。Jupyter 执行则使用创建沙箱时返回的每沙箱
`X-Access-Token`。

一个配置好的 API key 就是一个信任域。持有该 key 的任何客户端都能列出、通过自定义
`/execute` 执行和删除该部署中的所有沙箱；这是单租户认证，不是租户隔离。测试里的
“沙箱隔离”只表示不同执行上下文不共享状态，不表示不同 key
或不同用户之间有授权边界。

## 创建准入、容量与指标

`MAX_CONCURRENT_SANDBOX_CREATIONS=0` 表示不限制并发创建；设为正数时，创建的
bundle/create/boot/init 工作受该上限约束，等待者按 FIFO 顺序准入。排队创建使用
`QUEUE_WAIT_TIMEOUT_MS`；超时会释放容量保留并返回
`429 too_many_requests`，已准入的 boot/init 不会被取消。

容量是已提交容量：`active + reserved + draining`。创建会先保留容量；`DELETE`
会立即从逻辑注册表移除沙箱并请求终止，但 worker 完成最终 shutdown 前仍属于
`draining`，不会释放给替代沙箱。

认证后的 `GET /internal/metrics` 是运维遥测，不属于 E2B 兼容 API 或 OpenAPI。
它仅返回汇总生命周期、容量和队列数据；`rollingLifecycleWindow` 是 60 秒生命周期
直方图，`lifetimeFinalWorkerTotals` 是适配器生命周期内单调累加的最终 worker
CPU/V8 总量。`bench_sandboxes.py` 会在 warmup
后和计时阶段后打印已净化的指标快照，查询不 计入测量 wall time。

覆盖范围：认证、有状态 JS/TS、get/list、环境变量优先级与还原、沙箱隔离、
用户错误与 API 错误的区分、删除、TTL 过期、超时上限拒绝。

### 四个必须知道的 SDK 事实

这些都是实测得出的，决定了套件为什么这样写：

1. **不要用 `debug=True`。** debug 模式下 SDK 会短路 `Sandbox._create`
   （`e2b/sandbox_sync/main.py`），伪造一个 `debug_sandbox_id` 并且**完全不调用
   控制平面**——它的语义是"连一个本地已在跑的 envd"，不是"连一个本地 API"。
   套件用显式 `api_url` 把控制面流量留在 localhost 且不走 TLS。
2. **本地 `run_code` 要覆盖 `_jupyter_url`。** 官方服务通过 wildcard host 路由
   Jupyter 流量；本项目只提供本地 HTTP `/sandboxes/{id}/jupyter/execute`。套件的
   `LocalSandbox` 和 `LocalAsyncSandbox` 子类把 `_jupyter_url`
   指向这个路径。创建响应只返回一次 `envdAccessToken`，SDK 随后用
   `X-Access-Token` 调用 Jupyter；GET/list 不会 暴露该令牌。
3. **省略 `language` 在这里表示 JavaScript。** 这与 E2B Code Interpreter
   通常默认 Python 不同。显式支持 `javascript` 和 `typescript`；Python 返回
   `unsupported_language`。`commands`、`files` 和 envd ConnectRPC
   仍未实现，`/execute` 仍是 SDK 没有对应方法的自定义端点。
4. **API key 最好形如 `e2b_` + 十六进制。** 较早的 SDK
   版本在**客户端**校验格式，像 `test-key` 这种值请求都发不出去，报的是 key
   格式错误、和适配器无关；用合规格式可以避免版本差异。

## 沙箱创建速度（`bench_sandboxes.py`）

和 `test_sdk.py` 用同一个适配器实例。每轮：SDK 创建沙箱 → 打一次 `POST /execute`
执行 `1 + 2` → `kill`。代码故意取最简，让耗时反映沙箱生命周期
本身而不是客体计算量；结果仍会校验，算错记成 error 而不是记成"很快"。

```bash
python3 tests/e2b/bench_sandboxes.py --count 1000 --concurrency 16
```

用完即 kill，所以同时存活的沙箱数 ≤ `--concurrency`，`--count` 可以远超
`MAX_CONCURRENT_SANDBOXES`（128）。1000 个沙箱不可能同时存活：每个是一个常驻
user worker，占 `SANDBOX_MEMORY_MB`（默认 128MB）。

本机实测（macOS arm64，10 核 = 4 性能 + 6 能效，release 构建，`--count 1000`，
全部 `ok=1000 failed=0`）：

```text
concurrency=16   wall=3.2s   throughput=314.7 sandboxes/s
phase           p50      p90      p99      max     mean   (ms)
create         30.0     40.6     60.9     73.2     31.3
execute        12.8     20.6     27.5     32.8     13.2
kill            5.2     11.9     18.6     22.9      6.1

concurrency=8    wall=3.6s   throughput=279.8 sandboxes/s
phase           p50      p90      p99      max     mean   (ms)
create         19.1     24.1     31.7     35.6     19.8
execute         5.4      9.6     14.8     17.0      6.1
kill            1.9      4.8     11.6     13.6      2.6

concurrency=1（无争用下限，30 次）
create p50 12.3     execute p50 2.1     kill p50 0.6
```

即**创建一个沙箱约 12ms**（无争用），并发 16 下 30ms，执行和销毁都是毫秒级。

### 为什么之前是 0.7 秒

适配器最初给 `EdgeRuntime.userWorkers.create` 传的是 `servicePath`（目录）。这条
路径下运行时**每次创建都重新生成 eszip**（`crates/base/src/runtime/mod.rs` 里
`maybe_eszip` 为空的分支）：解析模块图、转译 TS、并把 npm 缓存递归读成一份新的
vfs。`sample` 抓正在建沙箱的进程，热点就在
`generate_binary_eszip → build_npm_vfs → VfsBuilder::add_dir_recursive`。

拆开计时：单次 create 343ms 里 `userWorkers.create` 338ms、init 探针 4ms、适配器
自身的注册表/校验/HTTP 不到 1ms。代价与执行器代码量无关——换成单文件服务后 boot
仍是 322ms。真正的来源是**根 `deno.json` 的 workspace**：它把 `./examples` 和
`./crates/base/test_cases` 全部成员的 npm 依赖都拉进解析范围，而执行器自己只
import `node:` 内置和相对文件。用空 `DENO_DIR` 对照：

```text
                 首次 create   写入 DENO_DIR
每次生成 eszip      7.6s          26MB
bundle 一次复用      29ms         396KB
```

现在适配器首次创建时用 `EdgeRuntime.bundle()` 打包一次并缓存，之后每次 create 传
`maybeEszip`，boot 从 338ms 降到 7ms；打包本身只要约 17ms，所以连首次 create 也
只有 29ms。同一台机器同一份代码的前后对比：

```text
                     create p50        1000 个沙箱 (c16)
每次生成 eszip        343ms (c1)        47.6s   21.0/s
bundle 一次复用        12.3ms (c1)        3.2s  314.7/s
```

`EdgeRuntime.bundle` 只解析入口的图，因此跳过了 workspace 配置，也跳过了逐次路径
对 user worker 施加的类型检查。执行器要一直只依赖 `node:` 内置和相对文件；要用
npm 依赖或 import map 别名，就得把配置传给 bundle 调用。bundle 入口会规范化后
限制在仓库根目录（包含 `deno.json` 的祖先）内，符号链接也不能越界；只有 main
worker 能调用 bundle。

TypeScript 转译只开放给显式选择 `allowTranspile` 的执行器 worker。转译输入最多
16 KiB。解析使用独立大栈线程，并由进程级信号量把并发数限制为机器可用并行度；
其他 user worker 不能调用这个 op。

剩下的并发放大（c1 12.3ms → c16 30.0ms）是 CPU 争用，机器只有 4 个性能核；
`EDGE_RUNTIME_WORKER_POOL_SIZE` 提到 32
对旧路径无改善，说明瓶颈从来不是池线程数。

主要参数：`--count`、`--concurrency`、`--timeout`（沙箱 TTL 秒）、`--base-url`。

详见 `examples/e2b-adapter/README.md`。

### Linux host profiling

以下手工流程把 `perf stat` 只附着到 Edge Runtime 进程，不计入 Python SDK/client
的 CPU；benchmark 输出仍会包含 adapter 的启动阶段指标。需要 Linux
`perf`（通常是与运行内核匹配的 `linux-tools` 包）和对目标进程的
`perf_event_open` 权限。

```bash
cargo build --release
python3 -m pip install e2b
export E2B_API_KEY=e2b_0000000000000000000000000000000000

runtime_pid=""
perf_pid=""
cleanup() {
  [ -z "$perf_pid" ] || kill -INT "$perf_pid" 2>/dev/null || true
  [ -z "$perf_pid" ] || wait "$perf_pid" 2>/dev/null || true
  [ -z "$runtime_pid" ] || kill -TERM "$runtime_pid" 2>/dev/null || true
  [ -z "$runtime_pid" ] || wait "$runtime_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

./target/release/edge-runtime start --ip 127.0.0.1 --port 9998 \
  --main-service ./examples/e2b-adapter &
runtime_pid=$!

ready=0
for _ in $(seq 1 100); do
  if curl -fsS -H "x-api-key: $E2B_API_KEY" \
    http://127.0.0.1:9998/v2/sandboxes >/dev/null; then
    ready=1
    break
  fi
  if ! kill -0 "$runtime_pid" 2>/dev/null; then
    echo "edge-runtime exited before becoming ready" >&2
    exit 1
  fi
  sleep 0.05
done
[ "$ready" = 1 ] || {
  echo "edge-runtime did not become ready within 5 seconds" >&2
  exit 1
}

perf stat -p "$runtime_pid" \
  -e task-clock,minor-faults,context-switches &
perf_pid=$!

python3 tests/e2b/bench_sandboxes.py --count 1000 --concurrency 16

grep -E '^Vm(RSS|HWM):' "/proc/$runtime_pid/status"
kill -INT "$perf_pid"
wait "$perf_pid" || true
perf_pid=""
```

`VmHWM` 包含进程启动和 warmup，`VmRSS` 是读取时的进程总 RSS；二者都 不是单
sandbox 的内存值。只比较相同 release 二进制、CPU/cgroup 配额、 THP 设置、warmup
和并发度下的运行。容器常因 `perf_event_paranoid`、 seccomp 或缺少 `CAP_PERFMON`
拒绝 perf 附着；应在同用户宿主进程上采集， 而不是为 benchmark 扩大生产容器权限。

## 适用范围

`test_executor.py` 测的是**内层执行器**，绕过控制平面直接打
`/internal/execute`。公开的 E2B 兼容 API 由 `test_sdk.py` 覆盖。

| 能力                                                  | 状态                                                                                    |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------- |
| 持久化 JS/TS 执行（`/internal/execute`）              | 已实现，`test_executor.py` 覆盖                                                         |
| `POST /sandboxes`、`DELETE`、`/timeout`、`X-API-Key`  | 已实现，`test_sdk.py` 用官方 SDK 覆盖                                                   |
| Jupyter `run_code`、`X-Access-Token`、同步/异步客户端 | 已实现，本地通过 `_jupyter_url` 覆盖                                                    |
| envd ConnectRPC、`files`、`commands`                  | **未实现**                                                                              |
| 沙箱内执行 Python 代码                                | **明确不支持**（规范 §3 non-goal），传 `language: "python"` 返回 `unsupported_language` |
| E2B 生产 wildcard-host DNS/TLS 路由                   | **未实现**，只支持上述本地 HTTP Jupyter URL                                             |

也就是说：这里的 Python 只是**测试驱动语言**，不是被执行的沙箱语言；`run_code`
里的省略语言专门映射为 JavaScript。

## 前置条件

1. **必须先构建**，而且要在改动 Rust 后重新构建：

   ```bash
   cargo build
   ```

   陈旧的 `target/debug/edge-runtime` 会报
   `Unknown built-in "node:" module: vm`——这不是代码 bug，而是二进制里没有
   `ext/node` 的 `vm` 白名单改动。

2. macOS 需要 OpenBLAS：`brew install openblas`

## 启动被测服务

测试夹具 `crates/base/test_cases/e2b-executor-harness` 用
`servicePath: '../../examples/e2b-executor'`，这是**相对当前工作目录**的路径。
所以必须在 `crates/base` 目录下启动，否则找不到执行器：

```bash
cd crates/base
RUST_LOG=error ../../target/debug/edge-runtime \
  start --main-service ./test_cases/e2b-executor-harness -p 9998
```

夹具为了让测试跑得快，把执行超时压到很小的值：
`EXECUTION_TIMEOUT_MS=1000`、`EXECUTOR_ASYNC_TIMEOUT_MS=200` （生产默认是 10000
/ 12000）。

## 运行测试

在另一个终端，仓库根目录下：

```bash
python3 tests/e2b/test_executor.py
```

预期输出结尾：

```text
23/23 passed, 0 skipped
```

可选参数：

```bash
python3 tests/e2b/test_executor.py --base-url http://localhost:9000
python3 tests/e2b/test_executor.py -k typescript      # 按名字过滤
```

装了 pytest 的话也可以直接 `pytest tests/e2b/test_executor.py`， 用
`E2B_BASE_URL` 环境变量指定地址。

## 手工验证核心用例

最重要的一条是跨请求状态保持：

```bash
for code in 'let x = 1' 'x++' 'x'; do
  printf '%s -> ' "$code"
  curl -s -X POST http://localhost:9998/internal/execute \
    -H 'content-type: application/json' \
    -H 'x-sandbox-id: demo' \
    -d "{\"code\":\"$code\",\"language\":\"javascript\"}"
  echo
done
```

实际输出：

```text
let x = 1 -> {"result":null,"result_type":"undefined",...,"error":null}
x++       -> {"result":1,"result_type":"number",...,"error":null}
x         -> {"result":2,"result_type":"number",...,"error":null}
```

不同 `x-sandbox-id` 对应不同的 User Worker，互相不可见。

## 覆盖内容

- **状态保持**：基本类型、对象、函数、类实例；Worker 被复用（创建次数 = 1）
- **隔离**：不同 sandbox 之间变量不可见
- **TypeScript**：状态跨请求保持、`interface` 被擦除、与 JS 共享同一上下文
- **输出与序列化**：console 捕获、BigInt/Map/循环引用/函数不会破坏响应
- **环境变量**：sandbox 基线、单次请求覆盖、下一次自动还原、主 worker
  密钥不可见、非字符串值被拒（400）
- **错误与超时**：runtime error 不破坏状态、同步死循环超时后沙箱仍可用、Promise
  结果、悬挂 Promise 触发异步超时、compile error 与 runtime error 区分
- **语言**：`python` 被拒且不执行代码

## 本测试发现并已修复的缺陷

### 严重：嵌套 op 会让整个进程 abort（已修复）

现象：沙箱里执行 `new URL(...)` 或 `fetch(...)` 时，整个 edge-runtime 进程直接
abort（不只是当前 worker）：

```text
thread '<unnamed>' panicked at deno_url-0.182.0/lib.rs:68:1:
already borrowed: BorrowMutError
panic in a function that cannot unwind
```

根因：同步 op `op_vm_script_run_in_context` 之前签名是 `state: &mut OpState`，
deno_core 会在**整个 op 执行期间**持有 OpState
的可变借用——包括用户代码运行的那段 时间。于是嵌套的 `op_url_parse`
再次可变借用同一个 OpState 就 panic，而该函数不可 unwind，只能 abort
进程。任意一行沙箱代码即可打死整个运行时。

修复：改为
`Rc<RefCell<OpState>>`，把权限检查的借用限制在用户代码执行**之前**结束。 这与
Task 2 对 `op_vm_create_script` / `op_vm_create_context` 的处理方式一致，
且一次性修好了所有嵌套
op（URL、fetch、timers、crypto……），而不是逐个删全局对象。

回归覆盖：

- Rust：`test_e2b_executor_survives_nested_ops_from_user_code`
- Python：`test_nested_ops_do_not_abort_the_runtime`

回退修复后，Rust 测试进程会以 `SIGABRT` 退出，可据此确认该测试真的能捕获此缺陷。

## 当前 vm 边界

裸 `node:vm` 上下文会从宿主 global 派生属性，因此执行器在创建上下文后立即封闭
全局对象。非允许项会被不可配置地遮蔽：

```text
typeof fetch       -> "undefined"
typeof Deno        -> "undefined"
typeof WebSocket   -> "undefined"
typeof EdgeRuntime -> "undefined"
```

URL、文本编解码、计时器、console 和 `process.env` 通过最小 facade 提供。网络和
文件系统仍由 user worker 权限与 JS denylist 作为第二层保护；vm 边界本身不被当成
阻止执行器 realm 逃逸的唯一安全边界。
