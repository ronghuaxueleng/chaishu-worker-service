# Worker 节点配置要求与部署指南

## 最低配置要求

### 1. 硬件配置

#### 🖥️ **最低配置**（单 Provider、2 进程）

| 资源 | 最低要求 | 推荐配置 | 说明 |
|-----|---------|---------|------|
| **CPU** | 2 核 | 4 核+ | 每个 Worker 进程约 0.5-1% CPU |
| **内存** | 2 GB | 4 GB+ | 每个进程约 80-100 MB |
| **磁盘** | 500 MB | 1 GB+ | 日志、Python 环境 |
| **网络** | 1 Mbps | 10 Mbps+ | 与 Redis/MySQL/Neo4j 通信 |

#### 🚀 **推荐配置**（多 Provider、10 进程）

| 资源 | 推荐配置 | 高性能配置 | 说明 |
|-----|---------|-----------|------|
| **CPU** | 8 核 | 16 核+ | 支持更多并发任务 |
| **内存** | 8 GB | 16 GB+ | 10 进程约 1-2 GB，留足系统缓存 |
| **磁盘** | 10 GB | 50 GB+ | SSD 更佳，存储日志和缓存 |
| **网络** | 100 Mbps | 1 Gbps+ | 低延迟内网环境 |

#### 📊 **内存计算公式**

```
最低内存 = 基础系统 + (进程数 × 单进程内存)
         = 512 MB + (进程数 × 100 MB)

示例：
- 2 进程: 512 MB + (2 × 100 MB) = 712 MB ≈ 1 GB
- 10 进程: 512 MB + (10 × 100 MB) = 1512 MB ≈ 2 GB
- 20 进程: 512 MB + (20 × 100 MB) = 2512 MB ≈ 3 GB

推荐内存 = 最低内存 × 2（预留系统和峰值使用）
```

---

### 2. 软件环境

#### 必需软件

| 软件 | 最低版本 | 推荐版本 | 说明 |
|-----|---------|---------|------|
| **Python** | 3.8+ | 3.10+ | 运行 Worker 脚本 |
| **pip** | 20.0+ | 最新 | 安装依赖包 |
| **Git** | 2.0+ | 最新 | 可选，用于拉取代码 |

#### Python 依赖包

```bash
# 核心依赖
Flask==2.3.3
SQLAlchemy==2.0.21
neo4j>=5.28.0
redis>=5.0.0
PyMySQL==1.1.0
httpx==0.25.0

# 完整依赖见 requirements.txt
```

---

### 3. 网络配置

#### 必需连接

| 服务 | 地址 | 端口 | 用途 | 延迟要求 |
|-----|------|------|------|---------|
| **Redis** | 可配置 | 6379 | 任务队列 | < 10ms |
| **MySQL** | 可配置 | 3306 | 数据库 | < 50ms |
| **Neo4j** | 可配置 | 7687 | 图数据库 | < 50ms |
| **AI API** | 外网 | 443 | AI 服务 | < 2000ms |

#### 网络拓扑

```
┌─────────────────────────────────────────────────────────┐
│                    Worker Node                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐       │
│  │ Worker-1   │  │ Worker-2   │  │ Worker-N   │       │
│  └────────────┘  └────────────┘  └────────────┘       │
└─────────────────────────────────────────────────────────┘
         │                │                │
         ├────────────────┼────────────────┤
         ↓                ↓                ↓
    ┌────────┐      ┌────────┐      ┌────────┐
    │ Redis  │      │ MySQL  │      │ Neo4j  │
    │ 队列   │      │ 数据库 │      │ 图数据 │
    └────────┘      └────────┘      └────────┘
         ↑                ↑                ↑
    内网高速连接（推荐 < 1ms 延迟）
```

---

### 4. 必需环境变量

#### ✅ **核心配置（必填）**

```bash
# Redis 配置
REDIS_HOST=192.168.1.100          # ✅ 必填
REDIS_PORT=6379                   # 可选，默认 6379
REDIS_PASSWORD=your-password      # 可选，无密码则留空
REDIS_DB=0                        # 可选，默认 0

# MySQL 配置
DB_HOST=192.168.1.100             # ✅ 必填
DB_PORT=3306                      # 可选，默认 3306
DB_USER=chaishu                   # 可选，默认 root
DB_PASSWORD=your-password         # 可选
DB_NAME=chaishu                   # 可选，默认 chaishu

# Neo4j 配置
NEO4J_URI=bolt://192.168.1.100:7687  # ✅ 必填
NEO4J_USER=neo4j                  # 可选，默认 neo4j
NEO4J_PASSWORD=your-password      # 可选
```

#### 🔧 **Worker 配置（可选）**

```bash
# 节点标识
KG_WORKER_NODE_NAME=worker-node-1   # 默认: worker-node

# 进程数量
KG_WORKERS_PER_PROVIDER=2           # 默认: 2
KG_MAX_TOTAL_PROCESSES=50           # 默认: 50
KG_MAX_PROCESSES_PER_PROVIDER=10    # 默认: 10

# 指定 Providers（留空则自动发现）
# KG_WORKER_PROVIDERS=deepseek-xxx,openai

# 日志级别
LOG_LEVEL=INFO                      # 默认: INFO
```

---

## 快速部署指南

### 方案 A: 最小化部署（1 节点 1 进程）

**适用场景**: 测试、开发环境

**配置**:
```bash
# 硬件
CPU: 1 核
内存: 1 GB
磁盘: 500 MB

# 环境变量
KG_WORKERS_PER_PROVIDER=1
```

**预期性能**:
- 并发任务: 1 个
- 处理速度: 1 章节/分钟（取决于 AI 响应速度）
- 适用 Provider 数: 1-2 个

---

### 方案 B: 标准部署（1 节点 2 进程/Provider）

**适用场景**: 小规模生产环境

**配置**:
```bash
# 硬件
CPU: 4 核
内存: 4 GB
磁盘: 10 GB

# 环境变量
KG_WORKERS_PER_PROVIDER=2
```

**预期性能**:
- 并发任务: 2-4 个（取决于 Provider 数量）
- 处理速度: 2 章节/分钟/Provider
- 适用 Provider 数: 2-5 个

---

### 方案 C: 高性能部署（多节点分布式）

**适用场景**: 大规模生产环境

#### 架构设计

```
┌────────────────┐
│  Web Node      │  4核 4GB  (Flask API + 1 Worker/Provider)
│  NODE_ROLE=web │
└────────────────┘
        │
        ├─────── Redis/MySQL/Neo4j
        │
┌────────────────┬────────────────┬────────────────┐
│ Worker Node 1  │ Worker Node 2  │ Worker Node 3  │
│ 8核 8GB        │ 8核 8GB        │ 8核 8GB        │
│ 4进程/Provider │ 4进程/Provider │ 4进程/Provider │
└────────────────┴────────────────┴────────────────┘
```

**配置**:
```bash
# Web Node
NODE_ROLE=web
KG_WORKERS_PER_PROVIDER=1

# Worker Nodes
NODE_ROLE=worker
KG_WORKERS_PER_PROVIDER=4
```

**预期性能**:
- 并发任务: 12-60 个（3节点 × 4进程 × Provider数）
- 处理速度: 12+ 章节/分钟/Provider
- 高可用性: 单节点故障不影响整体
- 可水平扩展: 增加节点立即生效

---

## 详细部署步骤

### Step 1: 准备环境

```bash
# 1.1 更新系统
sudo apt update && sudo apt upgrade -y  # Ubuntu/Debian
# 或
sudo yum update -y                       # CentOS/RHEL

# 1.2 安装 Python 3.10+
sudo apt install python3.10 python3.10-venv python3-pip -y

# 1.3 验证安装
python3 --version  # 应显示 3.10+
pip3 --version
```

---

### Step 2: 部署代码

```bash
# 2.1 创建工作目录
mkdir -p /opt/chaishu-worker
cd /opt/chaishu-worker

# 2.2 拉取代码（如果使用 Git）
git clone https://github.com/ronghuaxueleng/chaishu-vue3.git .

# 2.3 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 2.4 安装依赖
pip install -r requirements.txt
```

---

### Step 3: 配置环境变量

```bash
# 3.1 复制配置模板
cp .env.worker.example .env

# 3.2 编辑配置文件
nano .env  # 或使用 vim

# 3.3 最小配置示例
cat > .env << 'EOF'
# 必填项
REDIS_HOST=192.168.1.100
DB_HOST=192.168.1.100
NEO4J_URI=bolt://192.168.1.100:7687

# 可选项
KG_WORKERS_PER_PROVIDER=2
KG_WORKER_NODE_NAME=worker-node-1
LOG_LEVEL=INFO
EOF
```

---

### Step 4: 测试启动

```bash
# 4.1 手动启动（测试）
python worker.py

# 4.2 观察日志输出
# 应显示:
# ✓ Worker 模块导入成功
# ✓ Worker 进程已启动，节点进入运行状态
# Worker 节点运行中...

# 4.3 验证进程
ps aux | grep "python worker.py"  # 应显示多个进程

# 4.4 停止测试（Ctrl+C）
```

---

### Step 5: 生产部署（使用 Supervisor）

#### 5.1 安装 Supervisor

```bash
sudo apt install supervisor -y  # Ubuntu/Debian
# 或
sudo yum install supervisor -y  # CentOS/RHEL
```

#### 5.2 创建配置文件

```bash
sudo nano /etc/supervisor/conf.d/chaishu-worker.conf
```

**配置内容**:
```ini
[program:chaishu-worker-node1]
command=/opt/chaishu-worker/venv/bin/python /opt/chaishu-worker/worker.py
directory=/opt/chaishu-worker
user=www-data
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=30
stdout_logfile=/var/log/chaishu-worker-node1.log
stderr_logfile=/var/log/chaishu-worker-node1.error.log
environment=
    REDIS_HOST="192.168.1.100",
    DB_HOST="192.168.1.100",
    NEO4J_URI="bolt://192.168.1.100:7687",
    KG_WORKERS_PER_PROVIDER="2"
```

#### 5.3 启动服务

```bash
# 重新加载配置
sudo supervisorctl reread
sudo supervisorctl update

# 启动 Worker
sudo supervisorctl start chaishu-worker-node1

# 查看状态
sudo supervisorctl status

# 查看日志
sudo tail -f /var/log/chaishu-worker-node1.log
```

#### 5.4 常用管理命令

```bash
# 停止
sudo supervisorctl stop chaishu-worker-node1

# 重启
sudo supervisorctl restart chaishu-worker-node1

# 查看日志
sudo supervisorctl tail -f chaishu-worker-node1 stdout
```

---

## 配置验证

### 验证清单

| 验证项 | 检查方法 | 预期结果 |
|-------|---------|---------|
| **环境变量** | `env \| grep -E "REDIS\|DB_\|NEO4J"` | 显示所有必需变量 |
| **网络连通性** | `telnet $REDIS_HOST 6379` | 连接成功 |
| **Python 版本** | `python3 --version` | 3.8+ |
| **依赖包** | `pip list \| grep -E "Flask\|SQLAlchemy"` | 显示已安装 |
| **进程启动** | `ps aux \| grep worker.py` | 显示多个进程 |
| **日志输出** | `tail -f logs/worker.log` | 有活动日志 |

### 快速验证脚本

```bash
#!/bin/bash
# 保存为 check_worker.sh

echo "=== Worker 节点配置验证 ==="

# 1. 检查必需环境变量
echo "1. 检查环境变量..."
for var in REDIS_HOST DB_HOST NEO4J_URI; do
    if [ -z "${!var}" ]; then
        echo "   ❌ $var 未设置"
    else
        echo "   ✅ $var = ${!var}"
    fi
done

# 2. 检查网络连通性
echo "2. 检查网络连通性..."
nc -zv $REDIS_HOST 6379 2>&1 | grep -q "succeeded" && echo "   ✅ Redis 连接正常" || echo "   ❌ Redis 连接失败"
nc -zv $DB_HOST 3306 2>&1 | grep -q "succeeded" && echo "   ✅ MySQL 连接正常" || echo "   ❌ MySQL 连接失败"

# 3. 检查 Python 环境
echo "3. 检查 Python 环境..."
python3 --version && echo "   ✅ Python 已安装" || echo "   ❌ Python 未安装"

# 4. 检查依赖包
echo "4. 检查依赖包..."
pip list | grep -q "Flask" && echo "   ✅ Flask 已安装" || echo "   ❌ Flask 未安装"
pip list | grep -q "redis" && echo "   ✅ redis 已安装" || echo "   ❌ redis 未安装"

echo "=== 验证完成 ==="
```

---

## 资源规划建议

### 按任务量规划

| 任务量/天 | 节点数 | 每节点进程 | 总进程数 | 内存需求 |
|----------|--------|-----------|---------|---------|
| < 100 章节 | 1 | 2 | 2 | 2 GB |
| 100-500 | 1-2 | 3-5 | 6-10 | 4-8 GB |
| 500-2000 | 2-4 | 5-8 | 20-32 | 16-32 GB |
| > 2000 | 5+ | 8-10 | 40+ | 40+ GB |

### 按 Provider 数量规划

```
总进程数 = Provider 数量 × KG_WORKERS_PER_PROVIDER

示例：
- 5 个 Provider，每个 2 进程 → 10 个总进程
- 10 个 Provider，每个 3 进程 → 30 个总进程
```

---

## 常见问题

### Q1: Worker 启动失败，提示缺少环境变量

**解决方案**:
```bash
# 检查 .env 文件
cat .env

# 确保必填项都存在
REDIS_HOST=...
DB_HOST=...
NEO4J_URI=...

# 重新加载环境变量
source .env
```

---

### Q2: 内存不足，进程被 OOM Killer 杀死

**解决方案**:
```bash
# 1. 查看内存使用
free -h

# 2. 减少进程数
export KG_WORKERS_PER_PROVIDER=1

# 3. 增加 swap（临时方案）
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

---

### Q3: 网络延迟高，任务处理缓慢

**解决方案**:
```bash
# 1. 检查延迟
ping $REDIS_HOST
ping $DB_HOST

# 2. 使用内网地址
# 将外网 IP 改为内网 IP

# 3. 部署在同一个数据中心/可用区
```

---

## 总结

### 最低配置（测试环境）

```yaml
硬件:
  CPU: 1 核
  内存: 1 GB
  磁盘: 500 MB
  网络: 1 Mbps

软件:
  Python: 3.8+
  依赖: requirements.txt

环境变量:
  必填: REDIS_HOST, DB_HOST, NEO4J_URI
  可选: KG_WORKERS_PER_PROVIDER=1

预期性能:
  并发: 1 个任务
  速度: 1 章节/分钟
```

### 推荐配置（生产环境）

```yaml
硬件:
  CPU: 8 核
  内存: 8 GB
  磁盘: 50 GB SSD
  网络: 100 Mbps 内网

软件:
  Python: 3.10+
  管理: Supervisor

环境变量:
  必填: REDIS_HOST, DB_HOST, NEO4J_URI
  推荐: KG_WORKERS_PER_PROVIDER=4

预期性能:
  并发: 10-40 个任务
  速度: 4+ 章节/分钟/Provider
  高可用: 支持
```

### 关键要点

1. ✅ **最低 1GB 内存即可启动 1 个 Worker**
2. ✅ **必需 Redis、MySQL、Neo4j 网络连通**
3. ✅ **推荐使用 Supervisor 管理进程**
4. ✅ **内网部署性能更佳（延迟 < 10ms）**
5. ✅ **可随时水平扩展节点数量**

---

**实施日期**: 2025-10-17
**版本**: v1.0
**相关文档**:
- Worker分布式部署实施总结.md
- Worker节点任务分配与故障恢复机制.md
