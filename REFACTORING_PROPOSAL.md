# LightDAG 架构改进方案：框架级异常处理与生命周期管理

## 1. 概述

本项目（`light_dag`）是从业务项目 `news_briefing` 中独立拆分出的轻量级反应式 DAG 执行引擎。本方案旨在解决当前架构中的两个核心痛点：
1. **异常与错误处理在框架层的缺失**：当前依赖业务层通过自定义 `ErrorOperator` 与每算子手工 `try/catch` 绕过框架，存在错误吞没、竞态击穿以及样板代码冗余等问题。
2. **生命周期与资源清理的时序漏洞**：在提前报错或超时返回时，`dag.run()` 未等待已启动的在途异步任务（`exec`），导致外层提前销毁连接池/线程池，引发不可控的二次崩溃。

同时，本方案**保留并兼容**原设计中非常优雅的特性：**未选中的条件分支 Promise 保持悬空，在 DAG 运行结束后由 V8 垃圾回收器（GC）安全回收**。

---

## 2. 问题一：异常与错误处理 (Exception & Error Handling)

### 2.1 现状与根因分析

在原有实现中：
- 每个算子在其 `exec` 内部自行 `try/catch`，失败时返回 `{ branch: "error", output: { err_code: X, err_obj: err } }`。
- 在业务图定义中，所有算子的 `error` 分支均未重命名，隐式指向全局的 `err_code` 和 `err_obj`，由下游一个单例的 `ErrorOperator` 接收并处理。

该模式存在以下架构缺陷：

1. **异常“击穿”告警节点**：
   在 `dag.ts` 中，`runNode` 会在节点最外层 `catch` 时，将该节点的所有已知输出全部 `reject`：
   ```ts
   for (const global_name of all_global_output_names) {
       resolves.get(global_name)?.reject(err);
   }
   ```
   如果一个节点在 `exec` **之前**发生错误（例如配置缺失导致 `options_schema.parse()` 失败），由于 `err_code` 和 `err_obj` 也在该节点的输出列表中，这一步会直接将全局的 `err_code` Promise 变成 rejected 状态。下游正在等待 `err_code` 的 `ErrorOperator` 会直接因为输入 Promise 被拒绝而崩溃，**导致告警逻辑完全被击穿，通知根本发不出去**。
2. **并发错误吞没与不可控竞态**：
   DAG 中存在并行执行的分支（如 `fetch_news` 与 `history_news`）。如果两节点同时报错并尝试解析全局单例的 `err_code` / `err_obj` Promise，由于 Promise 状态机一旦决议就不可变，后报错的异常信息会被完全丢弃。
3. **严重的业务代码侵入与样板代码**：
   每个算子都需要复制一套捕获、包装结构，算子的输出 Schema 必须声明 `{ default: ..., error: ErrorInfoSchema }`，污染了业务契约。

---

### 2.2 解决方案：将异常处理提升为框架层能力

#### 核心设计思路
1. **算子纯粹化**：算子只定义业务输出分支，遇到异常直接 `throw`（或自然抛出未捕获异常）。算子基类提供可选的 `error_code?: number` 属性用于错误分类。
2. **错误包装与因果链（Error Chain）**：引入框架内置的 `DagNodeError` 类，记录是哪一个节点、哪个错误码、由什么底层原因抛出的异常。
3. **多米诺拒绝（Failure Propagation）**：当节点失败时，`runNode` 仅 `reject` 该节点的**正常业务输出**，下游依赖它的节点在 `await input` 时自动被拒绝并向上继续抛错。
4. **框架级错误拦截与通知**：
   - `LightDag` 实例支持注册 `onError?: (error: DagNodeError) => Promise<void> | void` 统一钩子。
   - `dag.run()` 最终直接将 `DagNodeError` 向上抛出（rethrow），让调用方（如 `run.ts`）在一个统一的 `try/catch` 中处理。

---

## 3. 问题二：生命周期与清理 (Lifecycle & Resource Cleanup)

### 3.1 现状与时序隐患解释

目前 `LightDag.run()` 的结束条件是：
```ts
const output_promises = outputs.map(name => tasks.get(name)!);
const resolved_outputs = await Promise.all(output_promises);
return output_values;
```
这会导致**提前返回漏洞**：
只要目标 `outputs` 完成（无论是成功还是由错误节点决议），或者触发了 `timeout`，`dag.run()` 就会立即退出执行。

#### 崩溃时序对比图：

```
[原有时序]
时间轴 -------------------------------------------------------------------->
history_news: |--查库失败--> [ErrorOp 决议 success=false]
dag.run():    |-------------- 返回退出 ----------------->
run.ts:                                  |-- cleanUp() 销毁数据库和线程池！--|
fetch_news:   |------------- 抓取网页中（耗时较长） ------------> [访问线程池报错: Thread pool destroyed!]
```

外部调用者收到 `dag.run()` 返回后，通常会在 `finally` 中执行资源回收（如 `execution_context.cleanUp()` 关闭数据库连接 `DataSource.destroy()`、销毁 `Piscina` 线程池）。此时后台还在执行的长任务去访问线程池或数据库，会直接抛出严重的二次崩溃。

---

### 3.2 方案设计：在途任务优雅收尾（Drain）与中断广播（AbortSignal）

#### 关键界定：区分“在途任务”与“未选中的悬空分支”
- **在途任务（Active Executions）**：已经成功拿到所有输入、**真正进入 `op.exec()` 运行中的任务**。这部分任务持有着系统资源（线程池、网络连接、DB 连接），必须等待其完成或响应中断，绝不能中途被清理逻辑直接拆毁底层资源。
- **未选中的悬空分支（Suspended Branches）**：从未得到输入的节点一直卡在 `await Promise.all(inputs)`。这部分协程**不计入在途任务**。当 `dag.run()` 退出后，图内部的 Map 和 Promise 引用被断开，它们由 V8 GC 自动清理，**完全不需要显式清理**。

#### 解决方案实现：
1. **引入活跃任务集合**：
   在 `dag.run()` 执行期间维护 `const active_executions = new Set<Promise<unknown>>();`。
   只有当算子通过了输入校验、即将调用 `op.exec()` 时，才将该 Promise 登记进集合；`exec()` 结束后从集合中移除。
2. **引入 `AbortController` 机制**：
   在 `OperatorArgs` 中追加 `signal?: AbortSignal`。当超时、或者有节点抛错失败时，立即调用 `abort_controller.abort()`。支持取消的算子（fetch 请求、AI SDK 调用等）可立即中断。
3. **退出时收尾（Graceful Drain）**：
   在 `dag.run()` 的 `finally` 中执行：
   ```ts
   await Promise.allSettled(active_executions);
   ```
   确保所有已进入执行态的算子都退出之后，`dag.run()` 才正式返回给上层调用者，彻底消除资源竞态。

---

### 3.3 算子级 AbortSignal 使用规范与最佳实践

在 DAG 运行过程中，很多算子内部包含网络请求、批处理或轮询任务。通过将框架注入的 `signal` 接入算子执行逻辑，可以在 DAG 超时或同图兄弟节点崩溃时，实现真正的“即时终止与释放”，避免无效消耗。

#### 3.3.1 传递方式与来源（无需业务输入）
`signal` **不需要**在算子的业务输入 `input_schema` 中定义。框架在执行节点调用 `op.exec(args)` 时，会自动将全局关联的 `AbortSignal` 注入至 `OperatorArgs.signal` 中。算子直接解构获取即可：
```ts
export default async function myExec({ inputs, requires, options, signal }: OperatorArgs): Promise<OperatorOutput> {
    // signal 为可选参数，不关心中断的算子可直接忽略
}
```

#### 3.3.2 触发中断的时机
1. **执行超时**：DAG 设置了 `timeout` 且耗时超限，框架调用 `abort_controller.abort(timeout_err)`；
2. **兄弟节点失败**：DAG 中任意节点抛出未捕获异常或校验失败，框架广播 `abort_controller.abort(err)`，立即通知其余正在执行的在途算子提早退出。

#### 3.3.3 三种典型算子终止模式

1. **模式一：直接转发给支持 signal 的底层异步 API（网络请求 / SDK）**
   现代 Node.js 的 `fetch`、各类 HTTP 客户端、OpenAI / AI SDK、数据库查询等大多原生支持 `signal`。发生中断时底层会自动取消网络请求并抛出 `AbortError`。
   ```ts
   export default async function fetchNewsExec({ inputs, signal }: OperatorArgs): Promise<OperatorOutput> {
       const url = inputs.url as string;
       // 原生 fetch 传入 signal，中断时底层会自动关闭 socket 连接并抛出 AbortError
       const response = await fetch(url, { signal });
       const data = await response.json();
       return { branch: "default", output: { items: data } };
   }
   ```

2. **模式二：长耗时循环 / 批处理中主动检查并退出（防空转）**
   对于大量数据的批处理或多步骤链式任务，在每次循环迭代开始前通过 `signal?.throwIfAborted()` 进行守卫检查：
   ```ts
   export default async function batchProcessExec({ inputs, signal }: OperatorArgs): Promise<OperatorOutput> {
       const items = inputs.items as string[];
       const results = [];
       for (const item of items) {
           // 检查中断：若图已超时或兄弟节点已崩溃，立即抛出 AbortError 停止后续循环
           signal?.throwIfAborted();
           results.push(await processItem(item));
       }
       return { branch: "default", output: { results } };
   }
   ```

3. **模式三：监听 abort 事件清理非原生异步任务（Timer / 长连接 / 进程）**
   对于自定义的异步 Promise、定时器或第三方未适配 signal 的组件，可通过事件监听清理资源：
   ```ts
   export default async function customWaitExec({ inputs, signal }: OperatorArgs): Promise<OperatorOutput> {
       return new Promise((resolve, reject) => {
           if (signal?.aborted) {
               return reject(signal.reason);
           }
           const timer = setTimeout(() => {
               resolve({ branch: "default", output: { done: true } });
           }, 5000);

           signal?.addEventListener("abort", () => {
               clearTimeout(timer);
               reject(signal.reason);
           }, { once: true });
       });
   }
   ```

---

## 4. 详细修改文件与代码方案

为完成上述改造，在 `light_dag` 仓库中需要修改以下文件：

### 4.1 修改文件 1：`src/operator.ts`

#### 改动要点：
1. `OperatorArgs` 增加 `signal: AbortSignal` 支持任务取消。
2. `Operator` 抽象基类增加可选属性 `error_code?: number`。
3. 算子不再强制声明 `error` 分支 schema。

#### 详细改动代码：

```ts
import { z } from "zod";

/** Schema for the argument bundle passed to an operator's exec function. */
export const OperatorArgsSchema = z.object({
    inputs: z.record(z.string(), z.unknown()).default({}),
    requires: z.record(z.string(), z.unknown()).default({}),
    options: z.record(z.string(), z.unknown()).default({}),
    // 增加 signal 字段（不可序列化对象通过 passthrough 或 custom 允许传递）
    signal: z.instanceof(AbortSignal).optional()
});
export type OperatorArgs = {
    inputs: Record<string, unknown>;
    requires: Record<string, unknown>;
    options: Record<string, unknown>;
    signal?: AbortSignal;
};

/** Schema for the value returned by an operator's exec function. */
export const OperatorOutputSchema = z.object({
    branch: z.string().nonempty().default("default"),
    output: z.record(z.string(), z.unknown())
});
export type OperatorOutput = z.infer<typeof OperatorOutputSchema>;

export abstract class Operator {
    name: string = "operator";

    /** 可选错误码，方便在框架报错日志与报警中快速定位算子类型 */
    error_code?: number;

    /** Zod schema defining the operator's DAG inputs. */
    abstract input_schema: z.ZodObject<any>;
    /** Zod schemas for output branches. 默认只需提供 default 分支. */
    abstract output_schemas: Record<string, z.ZodObject<any>>;
    /** Zod schema for context dependencies. */
    requires_schema: z.ZodObject<any> = z.object({});
    /** Zod schema for runtime options. */
    options_schema: z.ZodObject<any> = z.object({});

    input_map?: Record<string, string>;
    output_map?: Record<string, string>;
    requires_map?: Record<string, string>;
    options_map?: Record<string, string>;

    abstract exec:
        | ((args: OperatorArgs) => Promise<OperatorOutput>)
        | string;

    public setErrorCode(code: number): this {
        this.error_code = code;
        return this;
    }

    public setName(name: string): this {
        this.name = name;
        return this;
    }

    public mapInput(schema_name: string, global_name: string): this {
        if (!Object.hasOwn(this.input_schema.shape, schema_name))
            throw new Error(`Schema input name "${schema_name}" does not exist in operator "${this.name}"`);
        if (!this.input_map) this.input_map = {};
        this.input_map[schema_name] = global_name;
        return this;
    }

    public mapOutput(schema_name: string, global_name: string): this {
        const exists = Object.values(this.output_schemas).some(
            (schema) => Object.hasOwn(schema.shape, schema_name)
        );
        if (!exists)
            throw new Error(`Schema output name "${schema_name}" does not exist in operator "${this.name}"`);
        if (!this.output_map) this.output_map = {};
        this.output_map[schema_name] = global_name;
        return this;
    }

    public mapRequires(schema_name: string, global_name: string): this {
        if (!Object.hasOwn(this.requires_schema.shape, schema_name))
            throw new Error(`Schema requires name "${schema_name}" does not exist in operator "${this.name}"`);
        if (!this.requires_map) this.requires_map = {};
        this.requires_map[schema_name] = global_name;
        return this;
    }

    public mapOptions(schema_name: string, global_name: string): this {
        if (!Object.hasOwn(this.options_schema.shape, schema_name))
            throw new Error(`Schema options name "${schema_name}" does not exist in operator "${this.name}"`);
        if (!this.options_map) this.options_map = {};
        this.options_map[schema_name] = global_name;
        return this;
    }

    constructor(
        name?: string,
        input_map?: Record<string, string>,
        output_map?: Record<string, string>,
        requires_map?: Record<string, string>,
        options_map?: Record<string, string>
    ) {
        if (name) this.name = name;
        if (input_map) this.input_map = input_map;
        if (output_map) this.output_map = output_map;
        if (requires_map) this.requires_map = requires_map;
        if (options_map) this.options_map = options_map;
    }
}
```

---

### 4.2 修改文件 2：`src/dag.ts`

#### 改动要点：
1. 导出结构化异常类 `DagNodeError`。
2. 构造函数支持注册可选全局异常钩子 `onError`。
3. `run()` 内部创建 `AbortController`，并在超时或失败时触发 `abort()`。
4. 引入 `active_executions` 跟踪真正执行中的节点。
5. `finally` 块中 `await Promise.allSettled(active_executions)`，彻底解决清理竞态。
6. `runNode` 捕获到内部错误时包装为 `DagNodeError` 并仅 reject 其下游输出，同时通知 `abort_controller`。

#### 详细改动代码：

```ts
import { type Piscina } from "piscina";
import {
    type Operator,
    type OperatorArgs,
    type OperatorOutput,
    OperatorOutputSchema
} from "./operator.js";

/** 框架统一定义的节点执行异常类 */
export class DagNodeError extends Error {
    constructor(
        public readonly node_name: string,
        public readonly error_code: number | undefined,
        public override readonly cause: unknown
    ) {
        const causeMsg = cause instanceof Error ? cause.message : String(cause);
        super(`[LightDag] Node "${node_name}" failed${error_code !== undefined ? ` (code ${error_code})` : ""}: ${causeMsg}`);
        this.name = "DagNodeError";
    }
}

export type ErrorHandlerHook = (error: DagNodeError) => Promise<void> | void;

export class LightDag {
    private readonly operators: Map<string, Operator>;
    private readonly task_names: Set<string>;
    private debug: boolean;
    private timeout?: number | undefined;
    private error_handler?: ErrorHandlerHook;

    constructor(
        operators: Operator[],
        debug?: boolean,
        timeout?: number,
        error_handler?: ErrorHandlerHook
    ) {
        this.debug = debug ?? false;
        this.timeout = timeout;
        this.error_handler = error_handler;
        this.operators = new Map<string, Operator>();
        this.task_names = new Set<string>();

        for (const op of operators) {
            if (this.operators.has(op.name))
                throw new Error(`Duplicate operator name: "${op.name}"`);
            for (const name of Object.keys(op.input_schema.shape).map(n => op.input_map?.[n] ?? n))
                this.task_names.add(name);
            for (const [, outputs] of Object.entries(op.output_schemas))
                for (const name of Object.keys(outputs.shape).map(n => op.output_map?.[n] ?? n))
                    this.task_names.add(name);
            this.operators.set(op.name, op);
        }
    }

    public setDebug(enabled: boolean): void {
        this.debug = enabled;
    }

    public setTimeout(timeout: number): void {
        this.timeout = timeout;
    }

    public setOnError(handler: ErrorHandlerHook): void {
        this.error_handler = handler;
    }

    private log(...args: unknown[]): void {
        if (this.debug) console.log("[LightDag]", ...args);
    }

    public async run(
        inputs: Record<string, unknown>,
        outputs: string[],
        context?: Record<string, unknown>,
        options?: Record<string, unknown>,
        pool?: Piscina
    ): Promise<Record<string, unknown>> {
        // 1. 验证全局输入与输出
        if (this.debug) {
            const consumed = new Set<string>();
            const produced = new Set<string>();
            for (const op of this.operators.values()) {
                for (const name of Object.keys(op.input_schema.shape))
                    consumed.add(op.input_map?.[name] ?? name);
                for (const branchSchema of Object.values(op.output_schemas))
                    for (const name of Object.keys(branchSchema.shape))
                        produced.add(op.output_map?.[name] ?? name);
            }
            const provided_inputs = new Set(Object.keys(inputs));
            const requested_outputs = new Set(outputs);
            for (const name of produced) {
                if (!consumed.has(name) && !requested_outputs.has(name))
                    console.warn(`[LightDag] Warning: task "${name}" is produced but never consumed or requested`);
            }
            for (const name of consumed) {
                if (!produced.has(name) && !provided_inputs.has(name))
                    console.warn(`[LightDag] Warning: task "${name}" is consumed but never produced or provided`);
            }
        }

        // 2. 初始化各全局任务的 Promise 与 Resolver
        const tasks = new Map<string, Promise<unknown>>();
        const resolves = new Map<string, { resolve: (value: unknown) => void; reject: (reason: unknown) => void }>();
        for (const task of this.task_names) {
            const { promise, resolve, reject } = Promise.withResolvers<unknown>();
            // 防止未被消费的分支在 reject 时产生全局 UnhandledRejection 警告
            promise.catch(() => {});
            tasks.set(task, promise);
            resolves.set(task, { resolve, reject });
        }

        this.log("run() started");

        // 3. 追踪真正运行中的 exec 任务集合（在途任务）与 取消控制器
        const active_executions = new Set<Promise<unknown>>();
        const abort_controller = new AbortController();
        let timeout_id: NodeJS.Timeout | undefined;

        if (this.timeout) {
            timeout_id = setTimeout(() => {
                const timeout_err = new Error(`[LightDag] Execution timeout: ${this.timeout}ms`);
                abort_controller.abort(timeout_err);
                for (const { reject } of resolves.values()) {
                    reject(timeout_err);
                }
            }, this.timeout);
        }

        try {
            // 启动所有算子协程
            for (const op_name of this.operators.keys()) {
                this.runNode(
                    op_name,
                    tasks,
                    resolves,
                    active_executions,
                    abort_controller,
                    context,
                    options,
                    pool
                );
            }

            // 注入初始输入，启动第一批算子
            for (const [input_name, input_value] of Object.entries(inputs)) {
                const entry = resolves.get(input_name);
                if (!entry) throw new Error(`Input "${input_name}" not found in DAG`);
                entry.resolve(input_value);
            }

            // 监听目标输出 Promise
            const output_promises = outputs.map((output_name) => {
                const task = tasks.get(output_name);
                if (!task) throw new Error(`Requested output "${output_name}" does not exist in DAG`);
                return task;
            });

            const resolved_outputs = await Promise.all(output_promises);
            const output_values: Record<string, unknown> = {};
            for (let i = 0; i < outputs.length; i++) {
                output_values[outputs[i]!] = resolved_outputs[i];
            }
            this.log("run() completed successfully");
            return output_values;
        } catch (err) {
            // 发生错误时广播取消信号，通知支持中断的异步操作提早退出
            abort_controller.abort(err);

            // 触发全局异常钩子
            if (this.error_handler && err instanceof DagNodeError) {
                try {
                    await this.error_handler(err);
                } catch (handlerErr) {
                    console.error("[LightDag] Error handler failed:", handlerErr);
                }
            }
            throw err;
        } finally {
            if (timeout_id) clearTimeout(timeout_id);

            // 【关键核心】：等待所有已启动、正在跑 exec() 的在途任务平稳结束
            // 未选中的分支由于卡在 await inputs 上，没有进入 active_executions，无需等待，直接留给 GC
            if (active_executions.size > 0) {
                this.log(`Waiting for ${active_executions.size} active executions to drain...`);
                await Promise.allSettled(Array.from(active_executions));
            }
        }
    }

    private async runNode(
        name: string,
        tasks: Map<string, Promise<unknown>>,
        resolves: Map<string, { resolve: (value: unknown) => void; reject: (reason: unknown) => void }>,
        active_executions: Set<Promise<unknown>>,
        abort_controller: AbortController,
        context?: Record<string, unknown>,
        options?: Record<string, unknown>,
        pool?: Piscina
    ): Promise<void> {
        let all_global_output_names: string[] = [];
        try {
            const op = this.operators.get(name);
            if (!op) throw new Error(`Node "${name}" not found`);

            for (const branchSchema of Object.values(op.output_schemas))
                for (const key of Object.keys(branchSchema.shape))
                    all_global_output_names.push(op.output_map?.[key] ?? key);

            // 1. 等待上游输入（如果此节点属于未被触发的分支，将一直停在此处等待并最终被 GC 回收）
            const schema_input_names = Object.keys(op.input_schema.shape);
            const global_input_names = schema_input_names.map(n => op.input_map?.[n] ?? n);

            const op_requires: Record<string, unknown> = {};
            const schema_requires_names = Object.keys(op.requires_schema.shape);
            const global_requires_names = schema_requires_names.map(n => op.requires_map?.[n] ?? n);
            for (let i = 0; i < schema_requires_names.length; i++) {
                op_requires[schema_requires_names[i]!] = context?.[global_requires_names[i]!];
            }

            const op_options: Record<string, unknown> = {};
            const schema_options_names = Object.keys(op.options_schema.shape);
            const global_options_names = schema_options_names.map(n => op.options_map?.[n] ?? n);
            for (let i = 0; i < schema_options_names.length; i++) {
                op_options[schema_options_names[i]!] = options?.[global_options_names[i]!];
            }

            const parsed_requires = op.requires_schema.parse(op_requires);
            const parsed_options = op.options_schema.parse(op_options);

            const input_results = await Promise.all(
                global_input_names.map(global_name => {
                    const task = tasks.get(global_name);
                    if (!task) throw new Error(`Task "${global_name}" not found for node "${name}"`);
                    return task;
                })
            );

            // 若在等待输入期间 DAG 已被取消，则不再执行
            if (abort_controller.signal.aborted) {
                return;
            }

            const inputs: Record<string, unknown> = {};
            for (let i = 0; i < schema_input_names.length; i++) {
                inputs[schema_input_names[i]!] = input_results[i];
            }
            const parsed_inputs = op.input_schema.parse(inputs);

            this.log(`[${name}] START — inputs resolved`);

            // 2. 真正执行任务，登记进 active_executions
            const op_input_arg: OperatorArgs = {
                inputs: parsed_inputs,
                requires: parsed_requires,
                options: parsed_options,
                signal: abort_controller.signal
            };

            const exec_promise = (async () => {
                if (typeof op.exec === "string") {
                    if (!pool) throw new Error(`Worker pool not initialized for node ${name}`);
                    // Worker 线程中只传递可序列化的字段，避免 DataCloneError
                    return await pool.run(
                        { inputs: op_input_arg.inputs, options: op_input_arg.options },
                        { filename: op.exec }
                    );
                } else {
                    return await op.exec(op_input_arg);
                }
            })();

            active_executions.add(exec_promise);
            let raw_output: unknown;
            try {
                raw_output = await exec_promise;
            } finally {
                active_executions.delete(exec_promise);
            }

            // 3. 校验产出并 resolve 对应分支的输出 Promise
            const op_output: OperatorOutput = OperatorOutputSchema.parse(raw_output);
            if (!Object.hasOwn(op.output_schemas, op_output.branch)) {
                throw new Error(`Branch "${op_output.branch}" not found in node "${name}"`);
            }

            const parsed_output = op.output_schemas[op_output.branch]!.parse(op_output.output);
            const schema_output_names = Object.keys(op.output_schemas[op_output.branch]!.shape);
            const global_output_names = schema_output_names.map(n => op.output_map?.[n] ?? n);

            for (let i = 0; i < schema_output_names.length; i++) {
                const schema_name = schema_output_names[i]!;
                const global_name = global_output_names[i]!;
                const entry = resolves.get(global_name);
                if (!entry) throw new Error(`Resolve not found for output "${global_name}"`);
                entry.resolve(parsed_output[schema_name]);
            }

            this.log(`[${name}] END — branch: ${op_output.branch}`);
        } catch (err) {
            const op = this.operators.get(name);
            const dag_err = err instanceof DagNodeError ? err : new DagNodeError(name, op?.error_code, err);
            this.log(`[${name}] FAILED:`, dag_err.message);

            // 拒绝本节点的所有下游输出，向后传播错误
            for (const global_name of all_global_output_names) {
                resolves.get(global_name)?.reject(dag_err);
            }
        }
    }
}
```

---

### 4.3 修改文件 3：`src/index.ts`

导出新的类与类型定义：
```ts
export { LightDag, DagNodeError, type ErrorHandlerHook } from "./dag.js";
export {
    Operator,
    OperatorArgsSchema,
    OperatorOutputSchema,
    type OperatorArgs,
    type OperatorOutput
} from "./operator.js";
```

---

## 5. 对业务端（如 `news_briefing`）带来的巨大简化

当新版 `light-dag` 落地后，业务项目可以获得极大收益：

1. **废弃 `ErrorOperator` 与所有算子的样板 `try/catch`**：
   算子只需要实现业务逻辑：
   ```ts
   // 改造前：
   try {
       ...
       return { branch: "default", output: op_output };
   } catch (err) {
       return { branch: "error", output: { err_code: 1, err_obj: err } };
   }

   // 改造后：
   export class FetchNewsOperator extends Operator {
       name = "fetch_news";
       error_code = 1; // 仅需配置错误码
       ...
   }
   export default async function fetchNews({ inputs, requires }: OperatorArgs): Promise<OperatorOutput> {
       // 自然书写，出错直接 throw，无需 catch
       const result = await doFetch(...);
       return { branch: "default", output: { fetched_items: result } };
   }
   ```
2. **报警集中在 DAG 级别配置**：
   在组装图时直接注册：
   ```ts
   news_briefing_dag.setOnError(async (err: DagNodeError) => {
       await telegramErrorHandler.handleError({
           err_code: err.error_code ?? -1,
           err_obj: err.cause
       });
   });
   ```
3. **消除资源销毁竞争**：
   业务的 `run.ts` 在 `await dag.run(...)` 之后执行 `cleanUp()` 时，由于 DAG 已经等待在途任务优雅 Drain 完毕，不会再发生后台异步任务被腰斩崩溃的问题。
4. **算子零负担接入优雅中断（AbortSignal）**：
   算子按需选择性接入 `signal`。不关心中断的算子可完全忽略该参数；关心中断的算子（如网络爬虫、AI 接口调用、大循环处理）无需任何额外配置，直接透传 `signal` 给 `fetch` 或在循环中调用 `signal?.throwIfAborted()`，彻底避免无效的网络开销与内存空转。


