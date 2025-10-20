"""
知识图谱任务批次调度器
监控活跃批次状态，自动加载下一批任务
"""
import threading
import time
import logging
from typing import List

from src.utils.redis_client import get_redis_client
from .kg_task_queue_service import (
    load_next_batch,
    get_active_batch_length,
    get_main_queue_length,
    BATCH_SIZE
)

logger = logging.getLogger(__name__)

# 全局调度器线程
_scheduler_thread = None
_scheduler_running = False
_scheduler_lock = threading.Lock()

# 检查间隔（秒）
CHECK_INTERVAL = 5


def start_batch_scheduler():
    """启动批次调度器"""
    global _scheduler_thread, _scheduler_running

    with _scheduler_lock:
        if _scheduler_running:
            logger.warning("批次调度器已在运行")
            return

        _scheduler_running = True
        _scheduler_thread = threading.Thread(
            target=_batch_scheduler_loop,
            daemon=True,
            name="KG-Batch-Scheduler"
        )
        _scheduler_thread.start()
        logger.info("✅ 批次调度器已启动 (检查间隔: %d秒, 批次大小: %d)", CHECK_INTERVAL, BATCH_SIZE)


def stop_batch_scheduler():
    """停止批次调度器"""
    global _scheduler_running

    with _scheduler_lock:
        if not _scheduler_running:
            logger.warning("批次调度器未运行")
            return

        _scheduler_running = False
        logger.info("批次调度器已停止")


def _batch_scheduler_loop():
    """批次调度器主循环"""
    global _scheduler_running
    logger.info("[批次调度器] 开始运行，_scheduler_running=%s", _scheduler_running)

    try:
        # 初始化：为每个有主队列任务的Provider加载第一批
        redis_client = get_redis_client()
        if redis_client:
            active_providers = _get_active_providers(redis_client)
            if active_providers:
                logger.info(f"[批次调度器] 初始化: 发现 {len(active_providers)} 个活跃Provider")
                for provider in active_providers:
                    try:
                        loaded = load_next_batch(provider, BATCH_SIZE)
                        if loaded > 0:
                            logger.info(f"[批次调度器] 初始化: Provider '{provider}' 加载了 {loaded} 个任务")
                    except Exception as e:
                        logger.error(f"[批次调度器] 初始化Provider '{provider}' 失败: {e}", exc_info=True)
            else:
                logger.info("[批次调度器] 初始化: 当前没有待处理任务")
        else:
            logger.warning("[批次调度器] 初始化: Redis客户端获取失败")
    except Exception as e:
        logger.error(f"[批次调度器] 初始化阶段异常: {e}", exc_info=True)

    logger.info("[批次调度器] 进入主循环，_scheduler_running=%s", _scheduler_running)

    while _scheduler_running:
        try:
            redis_client = get_redis_client()
            if not redis_client:
                logger.debug("[批次调度器] Redis未连接，等待...")
                time.sleep(CHECK_INTERVAL)
                continue

            # 获取所有有主队列任务的Provider
            active_providers = _get_active_providers(redis_client)

            if not active_providers:
                # 没有待处理任务，休眠
                time.sleep(CHECK_INTERVAL)
                continue

            for provider in active_providers:
                try:
                    # 检查活跃批次是否为空
                    active_count = get_active_batch_length(provider)
                    main_count = get_main_queue_length(provider)

                    if active_count == 0 and main_count > 0:
                        # 活跃批次为空且主队列有任务，加载下一批
                        logger.info(
                            f"[批次调度器] Provider '{provider}' 活跃批次为空，"
                            f"准备加载下一批（主队列还有 {main_count} 个任务）"
                        )

                        loaded = load_next_batch(provider, BATCH_SIZE)

                        if loaded > 0:
                            logger.info(f"[批次调度器] ✅ Provider '{provider}' 已加载新批次: {loaded} 个任务")
                        else:
                            logger.warning(f"[批次调度器] ⚠️ Provider '{provider}' 加载批次失败或没有任务")

                    elif active_count > 0:
                        logger.debug(
                            f"[批次调度器] Provider '{provider}' 活跃批次还有 {active_count} 个任务在处理中"
                        )

                except Exception as e:
                    logger.error(f"[批次调度器] 处理Provider '{provider}' 时出错: {e}", exc_info=True)

            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            logger.error(f"[批次调度器] 主循环异常: {e}", exc_info=True)
            time.sleep(CHECK_INTERVAL)

    logger.info("[批次调度器] 已退出")


def _get_active_providers(redis_client) -> List[str]:
    """获取有待处理任务的Provider列表

    Args:
        redis_client: Redis客户端实例

    Returns:
        List[str]: Provider名称列表
    """
    try:
        main_queue_keys = redis_client.keys("kg:main_queue:*")
        providers = []

        for key in main_queue_keys:
            if isinstance(key, bytes):
                key = key.decode()

            provider = key.replace("kg:main_queue:", "")

            # 检查是否有任务
            queue_length = redis_client.llen(f"kg:main_queue:{provider}")
            if queue_length > 0:
                providers.append(provider)

        return providers

    except Exception as e:
        logger.error(f"获取活跃Provider失败: {e}", exc_info=True)
        return []


def get_scheduler_status() -> dict:
    """获取调度器状态

    Returns:
        dict: 调度器状态信息
    """
    return {
        'running': _scheduler_running,
        'check_interval': CHECK_INTERVAL,
        'batch_size': BATCH_SIZE,
        'thread_alive': _scheduler_thread.is_alive() if _scheduler_thread else False
    }
