#!/bin/bash
# Worker 节点启动脚本

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                                                               ║${NC}"
echo -e "${GREEN}║        知识图谱 Worker 节点 - 启动脚本                        ║${NC}"
echo -e "${GREEN}║                                                               ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo

# 检查 Python
if ! command -v python &> /dev/null && ! command -v python3 &> /dev/null; then
    echo -e "${RED}错误: 未找到 Python${NC}"
    exit 1
fi

PYTHON_CMD=$(command -v python3 || command -v python)
echo -e "${GREEN}✓${NC} Python: $($PYTHON_CMD --version)"

# 检查 .env 文件
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠${NC} 未找到 .env 文件"

    if [ -f ".env.example" ]; then
        echo -e "${YELLOW}→${NC} 复制 .env.example 到 .env"
        cp .env.example .env
        echo -e "${YELLOW}→${NC} 请编辑 .env 文件配置必要的环境变量"
        echo
        echo "必需配置:"
        echo "  - REDIS_HOST"
        echo "  - REDIS_PASSWORD"
        echo "  - DB_HOST"
        echo "  - DB_PASSWORD"
        echo "  - NEO4J_URI"
        echo "  - NEO4J_PASSWORD"
        echo
        read -p "配置完成后按回车继续..."
    else
        echo -e "${RED}✗${NC} 未找到 .env.example"
        exit 1
    fi
fi

# 加载环境变量
if [ -f ".env" ]; then
    echo -e "${GREEN}✓${NC} 加载 .env 配置"
    export $(cat .env | grep -v '^#' | xargs)
fi

# 检查必需的环境变量
REQUIRED_VARS=("REDIS_HOST" "DB_HOST" "NEO4J_URI")
MISSING_VARS=()

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        MISSING_VARS+=("$var")
    fi
done

if [ ${#MISSING_VARS[@]} -ne 0 ]; then
    echo -e "${RED}✗${NC} 缺少必需的环境变量:"
    for var in "${MISSING_VARS[@]}"; do
        echo "  - $var"
    done
    echo
    echo "请在 .env 文件中配置这些变量"
    exit 1
fi

# 检查依赖
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}⚠${NC} 未找到虚拟环境"
    read -p "是否创建虚拟环境? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${GREEN}→${NC} 创建虚拟环境..."
        $PYTHON_CMD -m venv venv
        echo -e "${GREEN}✓${NC} 虚拟环境创建完成"
    fi
fi

# 激活虚拟环境
if [ -d "venv" ]; then
    echo -e "${GREEN}→${NC} 激活虚拟环境..."
    source venv/bin/activate || . venv/bin/activate
    echo -e "${GREEN}✓${NC} 虚拟环境已激活"
fi

# 检查依赖包
echo -e "${GREEN}→${NC} 检查依赖包..."
if ! $PYTHON_CMD -c "import redis" &> /dev/null; then
    echo -e "${YELLOW}⚠${NC} 依赖包未安装"
    read -p "是否安装依赖? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${GREEN}→${NC} 安装依赖..."
        pip install -r requirements.txt
        echo -e "${GREEN}✓${NC} 依赖安装完成"
    fi
fi

# 创建日志目录
if [ ! -d "logs" ]; then
    echo -e "${GREEN}→${NC} 创建日志目录..."
    mkdir -p logs
fi

# 显示配置
echo
echo -e "${GREEN}当前配置:${NC}"
echo "  节点名称: ${KG_WORKER_NODE_NAME:-worker-node}"
echo "  每Provider进程数: ${KG_WORKERS_PER_PROVIDER:-2}"
echo "  Redis: ${REDIS_HOST}:${REDIS_PORT:-6379}"
echo "  MySQL: ${DB_HOST}:${DB_PORT:-3306}"
echo "  Neo4j: ${NEO4J_URI}"
echo

# 启动 Worker
echo -e "${GREEN}→${NC} 启动 Worker 节点..."
echo
$PYTHON_CMD worker.py
