# Kaggle 部署 Worker 节点指南

## 📋 概述

本指南介绍如何在 Kaggle 上免费部署知识图谱 Worker 节点。

### 为什么选择 Kaggle？

| 优势 | 说明 |
|-----|------|
| ✅ **完全免费** | CPU 环境免费使用（30小时/周） |
| ✅ **资源充足** | 4核 CPU + 16GB 内存 |
| ✅ **网络快速** | 适合调用外部 API |
| ✅ **零配置** | 无需自己搭建服务器 |
| ✅ **快速部署** | 5 分钟即可启动 |

### 适用场景

- ✅ 开发测试
- ✅ 小规模生产
- ✅ 临时扩容
- ✅ 成本敏感项目
- ⚠️ 不适合需要 7×24 持续运行的场景（除非使用付费版）

---

## 🚀 快速开始

### Step 1: 准备项目代码包

**⚠️ 重要：代码仓库是私有的，需要手动上传代码**

有两种方式上传代码到 Kaggle：

#### 方式一：ZIP 包上传（推荐）

1. 在本地打包项目代码：
   ```bash
   # 进入项目根目录
   cd chaishu-vue3
   
   # 创建 ZIP 包（排除不必要的文件）
   zip -r chaishu-vue3.zip . \
     -x "*.git*" \
     -x "*node_modules*" \
     -x "*frontend/dist*" \
     -x "*__pycache__*" \
     -x "*.pyc" \
     -x "*venv*"
   ```

2. ZIP 包会在稍后上传到 Kaggle Notebook

#### 方式二：Kaggle Dataset（适合多次使用）

1. 创建私有 Dataset：
   - 访问 [Kaggle Datasets](https://www.kaggle.com/datasets)
   - 点击 **New Dataset**
   - 上传 `chaishu-vue3.zip` 或整个项目文件夹
   - 设置为 **Private**
   - 填写标题：`chaishu-vue3`

2. Dataset 会在 Notebook 中引用

### Step 2: 上传 Notebook

1. 访问 [Kaggle](https://www.kaggle.com/)
2. 登录账号（没有则注册）
3. 点击 **Create** → **New Notebook**
4. 点击 **File** → **Import Notebook**
5. 上传 `kaggle-worker-notebook.ipynb` 文件

### Step 3: 上传代码（选择其一）

**方式一：上传 ZIP 包**
1. 在 Notebook 页面右侧点击 **+ Add Data**
2. 选择 **Upload** 标签
3. 上传前面准备的 `chaishu-vue3.zip`
4. 等待上传完成

**方式二：添加 Dataset**
1. 在 Notebook 页面右侧点击 **+ Add Data**
2. 搜索你的私有 Dataset `chaishu-vue3`
3. 点击 **Add** 添加到 Notebook

### Step 4: 配置环境

在 Notebook 右侧设置：
- **Accelerator**: CPU (不要选 GPU)
- **Internet**: ON（必需！）
- **Persistence**: OFF（可选）

### Step 5: 修改配置

找到 **Cell 3: 配置环境变量**，修改以下信息：

```python
# Redis 配置
os.environ['REDIS_HOST'] = 'your-redis-host.com'        # 改为你的 Redis 地址
os.environ['REDIS_PASSWORD'] = 'your-redis-password'    # 改为你的密码

# MySQL 配置
os.environ['DB_HOST'] = 'your-mysql-host.com'           # 改为你的 MySQL 地址
os.environ['DB_PASSWORD'] = 'your-mysql-password'       # 改为你的密码

# Neo4j 配置
os.environ['NEO4J_URI'] = 'bolt://your-neo4j-host.com:7687'  # 改为你的地址
os.environ['NEO4J_PASSWORD'] = 'your-neo4j-password'    # 改为你的密码
```

### Step 6: 运行 Notebook

1. 点击 **Run All** 或依次运行每个单元格
2. 等待依赖安装（约 1-2 分钟）
3. Cell 2 会自动检测并解压代码包
4. Worker 启动后会显示日志
5. 在前端页面的 "Worker节点" Tab 可以看到节点状态

---

## 📊 Notebook 单元格说明

### Cell 1: 安装依赖包
- 安装所有必需的 Python 包
- 耗时: ~1-2 分钟

### Cell 2: 上传并解压项目代码
- 自动检测 ZIP 包或 Dataset
- 解压到工作目录
- 验证关键文件
- 耗时: ~30 秒

### Cell 3: 配置环境变量 ⚠️
- **重要**: 必须修改为你的实际配置
- 包含 Redis、MySQL、Neo4j 连接信息

### Cell 4: 验证网络连通性（可选）
- 测试能否连接到各个服务
- 排查网络问题

### Cell 5: 创建必要的目录
- 创建日志目录

### Cell 6: 查看系统资源
- 显示 Kaggle 提供的资源
- 估算可运行的进程数

### Cell 7: 启动 Worker 节点 🚀
- **主要单元格**: 启动 Worker
- 会持续运行直到手动停止或超时

### Cell 8: 监控 Worker 状态（可选）
- 后台运行 Worker 并实时监控
- 显示 CPU、内存使用情况

### Cell 9: 查看 Worker 日志
- 排查问题时使用
- 显示最新的 50 行日志

### Cell 10: 停止 Worker
- 优雅停止所有 Worker 进程
- 清理资源

---

## ⚙️ 配置优化

### 进程数量调整

Kaggle CPU 环境资源：
- CPU: 4 核
- 内存: 16 GB
- 推荐配置: 8-10 进程/Provider

```python
# 标准配置（推荐）
os.environ['KG_WORKERS_PER_PROVIDER'] = '8'

# 保守配置（稳定优先）
os.environ['KG_WORKERS_PER_PROVIDER'] = '4'

# 激进配置（性能优先）
os.environ['KG_WORKERS_PER_PROVIDER'] = '12'
```

### 内存使用估算

```
内存需求 = 500MB (系统) + (进程数 × 100MB)

示例:
- 5 Providers × 8 进程 = 40 进程
- 内存: 500MB + (40 × 100MB) = 4.5GB
- 剩余: 16GB - 4.5GB = 11.5GB ✅ 足够
```

---

## 🔧 常见问题

### Q1: 为什么选择 CPU 而不是 GPU？

**答**: Worker 是 I/O 密集型，不需要 GPU
- Worker 主要工作: 网络请求、数据库操作
- GPU 擅长: 矩阵运算、深度学习
- 使用 CPU 更节省 GPU 配额

### Q2: 9 小时后会怎样？

**答**: Notebook 会自动停止
- 免费版: 最长运行 9 小时
- 需要手动重启 Notebook
- 正在处理的任务会中断（但已完成章节保留）
- 队列中的任务会由其他节点接管

### Q3: 如何实现 7×24 运行？

**方案 1: 多账号轮换**
```
账号 1: 周一-周三 (30小时)
账号 2: 周四-周六 (30小时)
账号 3: 周日 (备用)
```

**方案 2: 购买 Kaggle 付费版**
- 价格: $19.99/月
- 无时长限制

**方案 3: 使用云服务器**
- 阿里云/AWS/腾讯云
- 成本: $20-30/月

### Q4: 代码上传失败或找不到文件

**解决方案**:

1. **ZIP 包上传失败**
   - 确认文件名为 `chaishu-vue3.zip`
   - 检查文件大小（Kaggle 限制 500MB）
   - 尝试删除重新上传

2. **Dataset 找不到**
   - 确认 Dataset 已设为 Private
   - Dataset 标题必须包含 `chaishu-vue3`
   - 检查是否正确添加到 Notebook

3. **Cell 2 报错**
   - 查看错误信息中的路径
   - 确认 ZIP 包已解压成功
   - 检查 `worker.py` 是否存在

### Q5: 网络连接失败怎么办？

**排查步骤**:

1. 检查 Internet 是否开启
   - Notebook 右侧设置
   - Internet 必须为 ON

2. 验证服务器地址
   - 运行 Cell 4 测试连通性
   - 确认防火墙允许外部连接

3. 检查密码和端口
   - Redis: 默认 6379
   - MySQL: 默认 3306
   - Neo4j: 默认 7687

### Q6: 进程数太多导致内存不足

**解决方案**:
```python
# 减少进程数
os.environ['KG_WORKERS_PER_PROVIDER'] = '4'  # 从 8 改为 4

# 限制总进程数
os.environ['KG_MAX_TOTAL_PROCESSES'] = '30'  # 从 50 改为 30
```

### Q7: 如何查看任务处理情况？

**方法**:
1. 在主系统前端查看 "Worker节点" Tab
2. 查看 Kaggle Notebook 日志
3. 运行 Cell 9 查看详细日志

---

## 📈 性能监控

### 实时监控（Cell 8）

运行 Cell 8 可以看到：
```
[14:30:25] CPU:  15.2% | 内存:  4.2/16.0GB (26.3%) | 进程数: 41
```

### 资源使用正常范围

| 指标 | 正常范围 | 异常情况 |
|-----|---------|---------|
| CPU | 10-30% | > 50% 可能配置过多进程 |
| 内存 | 20-50% | > 80% 需要减少进程 |
| 进程数 | 20-80 | > 100 检查配置 |

---

## 🛡️ 安全建议

### 保护密码

**不要在 Notebook 中硬编码真实密码！**

推荐方式：
```python
# 方式 1: 使用 Kaggle Secrets（推荐）
from kaggle_secrets import UserSecretsClient
user_secrets = UserSecretsClient()
os.environ['REDIS_PASSWORD'] = user_secrets.get_secret('redis_password')

# 方式 2: 使用环境变量（运行时设置）
import getpass
os.environ['REDIS_PASSWORD'] = getpass.getpass('Redis Password: ')
```

### 设置 Kaggle Secrets

1. 进入 Notebook 设置
2. 点击 **Add-ons** → **Secrets**
3. 添加 secret:
   - Name: `redis_password`
   - Value: 你的密码

---

## 📊 成本对比

### Kaggle vs 云服务器

| 方案 | 成本/月 | CPU | 内存 | 时长限制 | 适用场景 |
|-----|--------|-----|------|---------|---------|
| **Kaggle 免费** | $0 | 4核 | 16GB | 30h/周 | 测试、小规模 |
| **Kaggle 付费** | $20 | 4核 | 16GB | 无限 | 中小规模 |
| **阿里云 ECS** | $25 | 2核 | 4GB | 无限 | 小规模生产 |
| **AWS t3.medium** | $30 | 2核 | 4GB | 无限 | 生产环境 |

**结论**: Kaggle 性价比最高！

---

## 🎯 最佳实践

### 1. 首次部署

```
第一周: 使用 Kaggle 免费版测试
  ├─ 验证功能正常
  ├─ 观察资源使用
  └─ 评估是否满足需求

如果满足需求:
  ├─ 继续使用免费版（小规模）
  ├─ 升级付费版（中规模）
  └─ 或迁移到云服务器（大规模）
```

### 2. 多账号管理

使用 3 个 Kaggle 账号轮流运行：
```
账号 1 (主): 周一 00:00 - 周三 06:00 (30小时)
账号 2 (副): 周三 06:00 - 周五 12:00 (30小时)
账号 3 (备): 周五 12:00 - 周日 18:00 (30小时)
```

### 3. 监控和告警

建议配置：
- 每天检查一次 Worker 状态
- 配置数据库监控（队列长度）
- 设置任务失败告警

### 4. 故障恢复

Kaggle 节点停止后：
- ✅ 队列中的任务会由其他节点接管
- ✅ 已完成的章节不会丢失
- ✅ 重启 Notebook 继续处理

---

## 📚 相关资源

### Notebook 文件
- 文件位置: `/app/kaggle-worker-notebook.ipynb`
- 上传到 Kaggle 即可使用

### 官方文档
- [Worker节点配置要求与部署指南](../Worker节点配置要求与部署指南.md)
- [Worker节点任务分配与故障恢复机制](../Worker节点任务分配与故障恢复机制.md)
- [Worker分布式部署实施总结](../Worker分布式部署实施总结.md)

### Kaggle 官方
- [Kaggle Notebooks 文档](https://www.kaggle.com/docs/notebooks)
- [Kaggle Secrets 使用](https://www.kaggle.com/docs/notebooks#secrets)

---

## ✅ 检查清单

部署前确认：

- [ ] 已注册 Kaggle 账号
- [ ] 已准备好 Redis/MySQL/Neo4j 服务器
- [ ] 已获取所有服务的连接信息和密码
- [ ] 已打包项目代码为 `chaishu-vue3.zip`
- [ ] 已下载 `kaggle-worker-notebook.ipynb`
- [ ] 已创建 Kaggle Secrets（推荐）

部署时确认：

- [ ] 已上传 ZIP 包或创建 Dataset
- [ ] Notebook 设置为 CPU + Internet ON
- [ ] Cell 2 成功解压代码
- [ ] Cell 3 配置已修改

部署后验证：

- [ ] Cell 4 网络测试全部通过
- [ ] Cell 7 Worker 成功启动
- [ ] 在主系统看到 Kaggle 节点
- [ ] 任务正常分配和处理
- [ ] 资源使用在正常范围

---

## 🎓 总结

### Kaggle 部署优势

1. ✅ **零成本启动** - 无需购买服务器
2. ✅ **快速部署** - 5 分钟完成
3. ✅ **资源充足** - 4核16GB够用
4. ✅ **灵活扩展** - 随时增减节点

### 适用人群

- 个人开发者
- 小团队
- 成本敏感项目
- 需要快速验证的场景

### 推荐策略

```
阶段 1 (测试): Kaggle 免费版
  ↓
阶段 2 (小规模生产): Kaggle 付费版 或 多账号
  ↓
阶段 3 (大规模生产): 云服务器 + Kaggle 混合
```

---

**版本**: v1.0
**更新日期**: 2025-10-17
