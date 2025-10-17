# 知识图谱 Worker 服务 - Docker 部署指南

## 快速开始

### 1. 准备环境变量

复制并编辑环境变量文件：

```bash
cp .env.example .env
vim .env  # 修改配置
```

### 2. 构建镜像

```bash
docker build -t kg-worker:latest .
```

### 3. 运行容器

**方式一：使用 docker run**

```bash
docker run -d \
  --name kg-worker-1 \
  --restart unless-stopped \
  -e REDIS_HOST=your-redis-host \
  -e REDIS_PASSWORD=your-redis-password \
  -e DB_HOST=your-mysql-host \
  -e DB_PASSWORD=your-mysql-password \
  -e NEO4J_URI=bolt://your-neo4j-host:7687 \
  -e NEO4J_PASSWORD=your-neo4j-password \
  -e KG_WORKERS_PER_PROVIDER=4 \
  -v $(pwd)/logs:/app/logs \
  kg-worker:latest
```

**方式二：使用 docker-compose**

```bash
# 创建网络（首次）
docker network create chaishu-network

# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down
```

## 多节点部署

### 方式一：同一主机多容器

修改 `docker-compose.yml`：

```yaml
services:
  kg-worker-1:
    build: .
    container_name: kg-worker-1
    environment:
      KG_WORKER_NODE_NAME: docker-worker-1
    # ... 其他配置

  kg-worker-2:
    build: .
    container_name: kg-worker-2
    environment:
      KG_WORKER_NODE_NAME: docker-worker-2
    # ... 其他配置
```

启动：
```bash
docker-compose up -d
```

### 方式二：不同主机部署

在每台主机上：

**主机 1**:
```bash
export KG_WORKER_NODE_NAME=host1-worker
docker run -d --name kg-worker-host1 \
  -e KG_WORKER_NODE_NAME=$KG_WORKER_NODE_NAME \
  # ... 其他环境变量
  kg-worker:latest
```

**主机 2**:
```bash
export KG_WORKER_NODE_NAME=host2-worker
docker run -d --name kg-worker-host2 \
  -e KG_WORKER_NODE_NAME=$KG_WORKER_NODE_NAME \
  # ... 其他环境变量
  kg-worker:latest
```

## 资源限制

### Docker Run

```bash
docker run -d \
  --name kg-worker \
  --cpus="4" \
  --memory="4g" \
  --memory-swap="4g" \
  # ... 其他配置
  kg-worker:latest
```

### Docker Compose

已在 `docker-compose.yml` 中配置：

```yaml
deploy:
  resources:
    limits:
      cpus: '4'
      memory: 4G
    reservations:
      cpus: '1'
      memory: 1G
```

## 日志管理

### 查看日志

```bash
# 容器日志
docker logs -f kg-worker-1

# 应用日志
docker exec kg-worker-1 tail -f /app/logs/worker.log
```

### 日志持久化

使用 volume 挂载：

```bash
-v $(pwd)/logs:/app/logs
```

### 日志轮转

编辑 `docker-compose.yml`：

```yaml
logging:
  driver: "json-file"
  options:
    max-size: "10m"
    max-file: "3"
```

## 健康检查

### 自动重启

```bash
docker run -d \
  --restart unless-stopped \
  # ... 其他配置
```

### 手动健康检查

```bash
# 检查容器状态
docker ps | grep kg-worker

# 检查进程
docker exec kg-worker-1 ps aux | grep python

# 检查内存
docker stats kg-worker-1
```

## 更新部署

### 重新构建镜像

```bash
# 拉取最新代码
git pull

# 重新构建
docker build -t kg-worker:latest .

# 停止旧容器
docker stop kg-worker-1
docker rm kg-worker-1

# 启动新容器
docker run -d --name kg-worker-1 \
  # ... 配置
  kg-worker:latest
```

### 使用 Docker Compose

```bash
# 重新构建并启动
docker-compose up -d --build

# 滚动更新（零停机）
docker-compose up -d --no-deps --build kg-worker
```

## 故障排查

### 容器无法启动

```bash
# 查看容器日志
docker logs kg-worker-1

# 查看容器详情
docker inspect kg-worker-1

# 交互式进入容器
docker run -it --rm \
  -e REDIS_HOST=... \
  kg-worker:latest /bin/bash
```

### 网络连接问题

```bash
# 测试 Redis 连接
docker exec kg-worker-1 ping -c 3 $REDIS_HOST

# 测试 MySQL 连接
docker exec kg-worker-1 nc -zv $DB_HOST 3306

# 测试 Neo4j 连接
docker exec kg-worker-1 nc -zv $NEO4J_HOST 7687
```

### 性能问题

```bash
# 实时资源监控
docker stats kg-worker-1

# 查看进程
docker exec kg-worker-1 ps aux

# 查看内存详情
docker exec kg-worker-1 free -h
```

## 生产环境最佳实践

### 1. 使用 Docker Secrets

```bash
# 创建 secrets
echo "your-redis-password" | docker secret create redis_password -
echo "your-mysql-password" | docker secret create mysql_password -

# 在 docker-compose.yml 中使用
services:
  kg-worker:
    secrets:
      - redis_password
      - mysql_password
    environment:
      REDIS_PASSWORD_FILE: /run/secrets/redis_password
```

### 2. 资源监控

使用 Prometheus + Grafana 监控：

```yaml
# docker-compose.yml
services:
  kg-worker:
    labels:
      - "prometheus.scrape=true"
      - "prometheus.port=8080"
```

### 3. 日志聚合

使用 ELK Stack 或 Loki：

```yaml
logging:
  driver: "fluentd"
  options:
    fluentd-address: localhost:24224
    tag: kg-worker
```

### 4. 自动扩容

使用 Docker Swarm 或 Kubernetes：

```bash
# Docker Swarm
docker service create \
  --name kg-worker \
  --replicas 3 \
  kg-worker:latest

# Kubernetes
kubectl scale deployment kg-worker --replicas=5
```

## 清理

```bash
# 停止并删除容器
docker-compose down

# 删除镜像
docker rmi kg-worker:latest

# 清理未使用的资源
docker system prune -a
```

## 参考资源

- [Docker 官方文档](https://docs.docker.com/)
- [Docker Compose 文档](https://docs.docker.com/compose/)
- [最佳实践指南](https://docs.docker.com/develop/dev-best-practices/)
