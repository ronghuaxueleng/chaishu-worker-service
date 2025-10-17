# 快速开始指南

## 🚀 5 分钟上手

### 1. 获取代码

```bash
# 从主项目复制 worker-service 目录
cp -r /path/to/chaishu-vue3/worker-service ./

# 或者如果单独创建了仓库
git clone https://github.com/ronghuaxueleng/chaishu-worker-service.git
cd worker-service
```

### 2. 配置环境

```bash
# 复制配置模板
cp .env.example .env

# 编辑配置（只需填写这3项！）
vim .env
```

**最少配置**:
```bash
REDIS_HOST=your-redis-host
DB_HOST=your-mysql-host
NEO4J_URI=bolt://your-neo4j-host:7687
```

### 3. 安装依赖

```bash
# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 4. 启动 Worker

**方式一：使用启动脚本（推荐）**
```bash
./start.sh
```

**方式二：直接运行**
```bash
python worker.py
```

### 5. 验证运行

看到以下输出表示成功：

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
✓ Worker 进程已启动，节点进入运行状态
Worker 节点运行中... (按 Ctrl+C 停止)
```

---

## 🐳 Docker 快速启动

```bash
# 1. 配置环境变量
cp .env.example .env
vim .env

# 2. 构建并启动
docker-compose up -d

# 3. 查看日志
docker-compose logs -f

# 4. 停止
docker-compose down
```

---

## ☁️ Kaggle 快速启动

### 准备工作

1. **打包代码**（本地执行）：
```bash
cd worker-service
zip -r worker-service.zip . -x "*.git*" "*venv*" "*logs*" "*.pyc"
```

2. **上传到 Kaggle**：
   - 登录 Kaggle
   - 创建新 Notebook
   - 右侧 **+ Add Data** → **Upload** → 上传 ZIP

### Kaggle Notebook 代码

```python
# Cell 1: 安装依赖
!pip install -q SQLAlchemy==2.0.21 PyMySQL==1.1.0 neo4j redis httpx python-dotenv

# Cell 2: 解压代码
import zipfile
with zipfile.ZipFile('/kaggle/input/worker-service.zip', 'r') as zip_ref:
    zip_ref.extractall('/kaggle/working/worker-service')
%cd /kaggle/working/worker-service

# Cell 3: 配置环境
import os
os.environ['REDIS_HOST'] = 'your-redis-host'
os.environ['REDIS_PASSWORD'] = 'your-password'
os.environ['DB_HOST'] = 'your-mysql-host'
os.environ['DB_PASSWORD'] = 'your-password'
os.environ['NEO4J_URI'] = 'bolt://your-neo4j:7687'
os.environ['NEO4J_PASSWORD'] = 'your-password'
os.environ['KG_WORKERS_PER_PROVIDER'] = '8'

# Cell 4: 启动 Worker
!python worker.py
```

---

## 📊 多节点部署

### 场景：2 台服务器 + 1 个 Kaggle

**服务器 1**:
```bash
export KG_WORKER_NODE_NAME=server1-worker
export KG_WORKERS_PER_PROVIDER=4
./start.sh
```

**服务器 2**:
```bash
export KG_WORKER_NODE_NAME=server2-worker
export KG_WORKERS_PER_PROVIDER=4
./start.sh
```

**Kaggle**:
```python
os.environ['KG_WORKER_NODE_NAME'] = 'kaggle-worker'
os.environ['KG_WORKERS_PER_PROVIDER'] = '8'
!python worker.py
```

所有节点自动连接到同一个 Redis 队列，实现负载均衡！

---

## ❓ 常见问题

### Q: 连接失败怎么办？

**A:** 检查防火墙和网络：

```bash
# 测试 Redis
telnet $REDIS_HOST 6379

# 测试 MySQL
telnet $DB_HOST 3306

# 测试 Neo4j
telnet $NEO4J_HOST 7687
```

### Q: 内存不够怎么办？

**A:** 减少进程数：

```bash
export KG_WORKERS_PER_PROVIDER=1
```

### Q: 如何查看日志？

**A:** 日志文件位置：

```bash
tail -f logs/worker.log
```

### Q: 如何停止 Worker？

**A:** 优雅停止：

```bash
# 按 Ctrl+C
# 或发送信号
kill -TERM $(pgrep -f worker.py)
```

---

## 📚 下一步

- [详细文档](README.md)
- [Docker 部署](docs/Docker部署指南.md)
- [Kaggle 部署](docs/Kaggle部署Worker节点指南.md)
- [项目结构](docs/项目结构说明.md)

---

## 🆘 获取帮助

遇到问题？

1. 查看 [README.md](README.md) 完整文档
2. 查看 [故障排查](docs/项目结构说明.md#-故障排查)
3. 提交 [Issue](https://github.com/ronghuaxueleng/chaishu-vue3/issues)

---

**祝使用愉快！** 🎉
