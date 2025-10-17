# 知识图谱 Worker 独立服务

这是从拆书系统(chaishu-vue3)中提取的独立 Worker 服务，专门用于分布式部署知识图谱构建任务。

## 📋 概述

Worker 服务通过 Redis 队列消费知识图谱任务，支持多节点分布式部署，实现自动负载均衡和故障转移。

### 核心特性

- ✅ **分布式部署** - 支持多节点横向扩展
- ✅ **自动负载均衡** - 基于 Redis 队列的 Pull 模型
- ✅ **故障自动转移** - 节点失败任务自动重新分配
- ✅ **进程保护机制** - 防止进程数爆炸
- ✅ **Provider 隔离** - 按 AI 服务商分配队列
- ✅ **实时状态监控** - 支持查询节点和任务状态

## 🚀 快速开始

### 1. 环境要求

**最低配置**:
- CPU: 1-2 核
- 内存: 1-2 GB
- Python: 3.8+

**推荐配置**:
- CPU: 4+ 核
- 内存: 4-8 GB
- Python: 3.10+

### 2. 安装依赖

```bash
# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 安装依赖
pip install -r requirements.txt
```

### 3. 配置环境变量

复制环境变量模板：

```bash
cp .env.example .env
```

编辑 `.env` 文件，配置以下**必需**项：

```bash
# Redis 配置
REDIS_HOST=your-redis-host
REDIS_PASSWORD=your-redis-password

# MySQL 配置
DB_HOST=your-mysql-host
DB_PASSWORD=your-mysql-password

# Neo4j 配置
NEO4J_URI=bolt://your-neo4j-host:7687
NEO4J_PASSWORD=your-neo4j-password
```

### 4. 启动 Worker

```bash
python worker.py
```

启动成功后会显示：

```
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║        拆书系统 - 知识图谱 Worker 独立节点                    ║
║        Chaishu Knowledge Graph Worker Node                    ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

知识图谱 Worker 节点启动
节点名称: worker-node-1
每Provider进程数: 2
...
✓ Worker 进程已启动，节点进入运行状态
Worker 节点运行中... (按 Ctrl+C 停止)
```

## ⚙️ 配置说明

### 环境变量

| 变量名 | 必需 | 默认值 | 说明 |
|--------|------|--------|------|
| `REDIS_HOST` | ✅ | - | Redis 主机地址 |
| `REDIS_PASSWORD` | ✅ | - | Redis 密码 |
| `DB_HOST` | ✅ | - | MySQL 主机地址 |
| `DB_PASSWORD` | ✅ | - | MySQL 密码 |
| `NEO4J_URI` | ✅ | - | Neo4j 连接 URI |
| `NEO4J_PASSWORD` | ✅ | - | Neo4j 密码 |
| `KG_WORKER_NODE_NAME` | ❌ | `worker-node` | 节点名称标识 |
| `KG_WORKERS_PER_PROVIDER` | ❌ | `2` | 每个 Provider 进程数 |
| `KG_WORKER_PROVIDERS` | ❌ | 自动发现 | 指定 Providers（逗号分隔） |
| `KG_MAX_TOTAL_PROCESSES` | ❌ | `50` | 最大总进程数 |
| `KG_MAX_PROCESSES_PER_PROVIDER` | ❌ | `10` | 单 Provider 最大进程数 |
| `LOG_LEVEL` | ❌ | `INFO` | 日志级别 |

### 进程数配置

**计算公式**:
```
内存需求 = 500MB (系统) + (进程数 × 100MB)
```

**配置示例**:

```bash
# 保守配置（1GB 内存）
KG_WORKERS_PER_PROVIDER=2
# 预计: 5 Providers × 2 = 10 进程，约 1.5GB

# 标准配置（4GB 内存，推荐）
KG_WORKERS_PER_PROVIDER=4
# 预计: 5 Providers × 4 = 20 进程，约 2.5GB

# 高性能配置（8GB 内存）
KG_WORKERS_PER_PROVIDER=8
# 预计: 5 Providers × 8 = 40 进程，约 4.5GB
```

## 📊 架构说明

### 工作原理

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Worker-1   │      │  Worker-2   │      │  Worker-3   │
│  (Node 1)   │      │  (Node 2)   │      │  (Kaggle)   │
└──────┬──────┘      └──────┬──────┘      └──────┬──────┘
       │                    │                    │
       └────────────────────┼────────────────────┘
                            │
                    ┌───────▼───────┐
                    │  Redis Queue  │
                    │               │
                    │  ┌─────────┐  │
                    │  │ openai  │  │
                    │  │ claude  │  │
                    │  │ zhipu   │  │
                    │  │  ...    │  │
                    │  └─────────┘  │
                    └───────────────┘
                            │
       ┌────────────────────┼────────────────────┐
       │                    │                    │
┌──────▼──────┐      ┌──────▼──────┐      ┌──────▼──────┐
│   MySQL     │      │   Neo4j     │      │  Frontend   │
│  (共享)     │      │  (共享)     │      │  (主系统)   │
└─────────────┘      └─────────────┘      └─────────────┘
```

### 关键特性

1. **Pull 模型**
   - Worker 主动从 Redis 队列拉取任务
   - 使用 `BRPOP` 原子操作避免竞争
   - 多节点自动负载均衡

2. **Provider 隔离**
   - 按 AI 服务商（OpenAI, Claude, 智谱等）划分队列
   - 每个 Provider 独立进程池
   - 支持暂停/恢复特定 Provider

3. **故障恢复**
   - 章节级事务，已完成章节不丢失
   - 节点失败，队列任务自动转移
   - 支持任务手动/自动重试

## 🛠️ 高级用法

### 指定 Providers

只为特定 AI 服务商启动 Worker：

```bash
export KG_WORKER_PROVIDERS=openai,claude
python worker.py
```

### Docker 部署

```bash
# 构建镜像
docker build -t kg-worker:latest .

# 运行容器
docker run -d \
  --name kg-worker-1 \
  -e REDIS_HOST=your-redis \
  -e REDIS_PASSWORD=your-password \
  -e DB_HOST=your-mysql \
  -e DB_PASSWORD=your-password \
  -e NEO4J_URI=bolt://your-neo4j:7687 \
  -e NEO4J_PASSWORD=your-password \
  -e KG_WORKERS_PER_PROVIDER=4 \
  kg-worker:latest
```

### Kaggle 部署

Worker 支持在 Kaggle 免费环境部署：

1. 上传项目代码到 Kaggle Notebook
2. 配置环境变量
3. 运行 `python worker.py`

详见: [Kaggle部署Worker节点指南](docs/Kaggle部署Worker节点指南.md)

### 多节点部署

在多台服务器上启动 Worker：

**服务器 1**:
```bash
export KG_WORKER_NODE_NAME=worker-node-1
python worker.py
```

**服务器 2**:
```bash
export KG_WORKER_NODE_NAME=worker-node-2
python worker.py
```

**Kaggle**:
```bash
export KG_WORKER_NODE_NAME=kaggle-worker-1
python worker.py
```

所有节点共享同一个 Redis 队列，自动负载均衡。

## 📈 监控与运维

### 查看 Worker 状态

在主系统前端 → **Worker 节点** Tab 可以看到：

- 各节点运行状态
- 每个 Provider 的进程数
- 队列长度
- 正在处理的任务

### 日志查看

Worker 日志输出到：
- **标准输出**: 实时查看
- **文件**: `logs/worker.log`

```bash
# 查看实时日志
tail -f logs/worker.log

# 查看错误日志
grep ERROR logs/worker.log
```

### 优雅停止

```bash
# Ctrl+C 或发送 SIGTERM
kill -TERM <pid>
```

Worker 会：
1. 停止接收新任务
2. 等待当前任务完成（最多 15 秒）
3. 优雅退出

## 🔧 故障排查

### 1. 连接失败

**症状**: `✗ Worker 模块导入失败`

**排查**:
```bash
# 测试 Redis 连接
redis-cli -h $REDIS_HOST -a $REDIS_PASSWORD ping

# 测试 MySQL 连接
mysql -h $DB_HOST -u root -p$DB_PASSWORD

# 测试 Neo4j 连接
cypher-shell -a $NEO4J_URI -u neo4j -p $NEO4J_PASSWORD
```

### 2. 内存不足

**症状**: 进程频繁崩溃，系统卡顿

**解决**:
```bash
# 减少进程数
export KG_WORKERS_PER_PROVIDER=2
```

### 3. 任务不执行

**排查**:
```bash
# 检查队列长度
redis-cli -h $REDIS_HOST -a $REDIS_PASSWORD
> LLEN kg:task:queue:openai
> LLEN kg:task:queue:claude
```

**可能原因**:
- Redis 队列为空（正常）
- Provider 被暂停（等待恢复）
- Worker 未启动或崩溃

## 📚 相关文档

- [Worker节点配置要求与部署指南](docs/Worker节点配置要求与部署指南.md)
- [Worker节点任务分配与故障恢复机制](docs/Worker节点任务分配与故障恢复机制.md)
- [Kaggle部署Worker节点指南](docs/Kaggle部署Worker节点指南.md)
- [Worker分布式部署实施总结](docs/Worker分布式部署实施总结.md)

## 🆘 获取帮助

如有问题，请提交 Issue 到主项目：
https://github.com/ronghuaxueleng/chaishu-vue3/issues

## 📄 许可证

MIT License

---

**版本**: v1.0
**更新日期**: 2025-10-17
