#!/usr/bin/env python3
"""
知识图谱 Worker 独立节点启动脚本
用于在多台服务器上分布式运行 Worker

使用环境变量配置：
- KG_WORKER_NODE_NAME: 节点名称（用于标识）
- KG_WORKERS_PER_PROVIDER: 每个 Provider 启动的进程数（默认2）
- KG_WORKER_PROVIDERS: 指定 Providers（逗号分隔，可选）
- KG_MAX_TOTAL_PROCESSES: 最大总进程数（默认50）
- KG_MAX_PROCESSES_PER_PROVIDER: 单Provider最大进程数（默认10）
- REDIS_HOST, DB_HOST, NEO4J_URI: 共享服务配置
"""
import os
import sys
import time
import logging
import signal
from pathlib import Path

# 添加父目录到 Python 路径（以便导入 src 模块）
parent_dir = Path(__file__).parent.parent.resolve()
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))
    print(f"✓ 已添加父目录到 Python 路径: {parent_dir}")

# 加载 .env 文件
try:
    from dotenv import load_dotenv
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✓ 已加载配置文件: {env_file}")
    else:
        print("⚠ 未找到 .env 文件，将使用环境变量")
except ImportError:
    print("⚠ python-dotenv 未安装，将使用环境变量")
except Exception as e:
    print(f"⚠ 加载 .env 文件失败: {e}")

# 配置日志
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
handlers = [logging.StreamHandler()]

# 尝试添加文件日志（可选）
try:
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    handlers.append(logging.FileHandler(log_dir / 'worker.log'))
except Exception as e:
    print(f"⚠ 无法创建日志文件: {e}，将仅使用控制台输出")

logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=handlers
)
logger = logging.getLogger(__name__)

# 优雅停止标志
_shutdown_requested = False


def signal_handler(signum, frame):
    """信号处理器：优雅停止"""
    global _shutdown_requested
    logger.info(f"收到信号 {signum}，准备停止 Worker 节点...")
    _shutdown_requested = True


def print_banner():
    """打印启动横幅"""
    banner = """
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║        拆书系统 - 知识图谱 Worker 独立节点                    ║
║        Chaishu Knowledge Graph Worker Node                    ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
"""
    print(banner)


def validate_environment():
    """验证必要的环境变量"""
    required_vars = ['REDIS_HOST', 'DB_HOST', 'NEO4J_URI']
    missing_vars = []

    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)

    if missing_vars:
        logger.error(f"缺少必要的环境变量: {', '.join(missing_vars)}")
        logger.error("请在 .env 文件或环境中配置这些变量")
        return False

    return True


def main():
    """Worker 节点主函数"""
    # 打印横幅
    print_banner()

    print(">>> [DEBUG] 开始初始化 Worker 节点...")
    sys.stdout.flush()

    # 注册信号处理
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        print(">>> [DEBUG] 信号处理器注册成功")
        sys.stdout.flush()
    except Exception as e:
        print(f">>> [DEBUG] 信号处理器注册失败（Kaggle环境可能不支持）: {e}")
        sys.stdout.flush()

    # 获取配置
    node_name = os.environ.get('KG_WORKER_NODE_NAME', 'worker-node')
    per_provider = int(os.environ.get('KG_WORKERS_PER_PROVIDER', '2'))
    providers = os.environ.get('KG_WORKER_PROVIDERS', None)
    max_total = os.environ.get('KG_MAX_TOTAL_PROCESSES', '50')
    max_per_provider = os.environ.get('KG_MAX_PROCESSES_PER_PROVIDER', '10')

    if providers:
        providers = [p.strip() for p in providers.split(',') if p.strip()]

    print(">>> [DEBUG] 配置读取完成，准备输出配置信息...")
    sys.stdout.flush()

    logger.info("=" * 60)
    logger.info("知识图谱 Worker 节点启动")
    logger.info(f"节点名称: {node_name}")
    logger.info(f"每Provider进程数: {per_provider}")
    logger.info(f"最大总进程数: {max_total}")
    logger.info(f"单Provider最大进程数: {max_per_provider}")

    if providers:
        logger.info(f"指定Providers: {providers}")
    else:
        logger.info("自动发现激活的 Providers")

    logger.info(f"Redis: {os.environ.get('REDIS_HOST')}")
    logger.info(f"MySQL: {os.environ.get('DB_HOST')}")
    logger.info(f"Neo4j: {os.environ.get('NEO4J_URI')}")
    logger.info("=" * 60)

    print(">>> [DEBUG] 配置信息输出完成")
    sys.stdout.flush()

    # 验证环境变量
    print(">>> [DEBUG] 开始验证环境变量...")
    sys.stdout.flush()

    if not validate_environment():
        logger.error("环境验证失败，退出")
        sys.exit(1)

    print(">>> [DEBUG] 环境变量验证通过")
    sys.stdout.flush()

    # 导入 Worker 模块
    print(">>> [DEBUG] 开始导入 Worker 模块...")
    sys.stdout.flush()

    try:
        from src.services.kg_task_worker import start_kg_task_workers, stop_all_workers
        logger.info("✓ Worker 模块导入成功")
        print(">>> [DEBUG] Worker 模块导入成功")
        sys.stdout.flush()
    except Exception as e:
        logger.error(f"✗ Worker 模块导入失败: {e}")
        print(f">>> [DEBUG] Worker 模块导入失败: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # 启动 Worker 进程
    print(">>> [DEBUG] 准备启动 Worker 进程...")
    sys.stdout.flush()

    try:
        logger.info("正在启动 Worker 进程...")
        start_kg_task_workers(
            providers=providers,
            per_provider_processes=per_provider
        )
        logger.info("✓ Worker 进程已启动，节点进入运行状态")
        print(">>> [DEBUG] Worker 进程已启动")
        sys.stdout.flush()
    except Exception as e:
        logger.error(f"✗ Worker 启动失败: {e}", exc_info=True)
        print(f">>> [DEBUG] Worker 启动失败: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # 显示活跃进程信息
    try:
        from src.services.kg_task_worker import get_worker_stats
        stats = get_worker_stats()
        logger.info(f"活跃进程统计: {stats}")
    except Exception as e:
        logger.warning(f"获取进程统计失败: {e}")

    # 启动心跳注册
    from datetime import datetime
    started_at = datetime.now().isoformat()

    def register_heartbeat():
        """注册节点心跳到 Redis"""
        try:
            from src.utils.redis_client import get_redis_client
            redis_client = get_redis_client()
            if not redis_client:
                return

            node_info = {
                'node_id': node_name,
                'node_type': 'worker',
                'workers_per_provider': per_provider,
                'pid': os.getpid(),
                'started_at': started_at,
                'last_heartbeat': datetime.now().isoformat()
            }

            key = f"kg:nodes:{node_name}"
            redis_client.hmset(key, node_info)
            redis_client.expire(key, 180)  # 3分钟过期
            logger.debug(f"节点心跳已注册: {node_name}")
        except Exception as e:
            logger.warning(f"注册节点心跳失败: {e}")

    # 首次注册心跳
    register_heartbeat()

    # 保持主进程运行，定期更新心跳
    logger.info("Worker 节点运行中... (按 Ctrl+C 停止)")
    heartbeat_counter = 0
    try:
        while not _shutdown_requested:
            time.sleep(1)
            heartbeat_counter += 1

            # 每60秒更新一次心跳
            if heartbeat_counter % 60 == 0:
                register_heartbeat()
                logger.debug(f"[心跳] Worker 节点 [{node_name}] 运行正常")
    except KeyboardInterrupt:
        logger.info("收到键盘中断")

    # 优雅停止
    logger.info("正在停止 Worker 节点...")
    try:
        stop_all_workers(timeout=15)
        logger.info("✓ Worker 节点已停止")
    except Exception as e:
        logger.error(f"✗ 停止过程出错: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
