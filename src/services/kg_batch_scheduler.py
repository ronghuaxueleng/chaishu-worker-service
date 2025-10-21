"""
知识图谱任务批次调度器（基于 APScheduler）
监控活跃批次状态，自动加载下一批任务

相比之前的守护线程实现，APScheduler 更可靠：
- 不受 Flask 热重载影响
- 更好的异常处理
- 支持复杂的调度策略
"""
import logging
from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.utils.redis_client import get_redis_client
from .kg_task_queue_service import (
    load_next_batch,
    get_active_batch_length,
    get_main_queue_length,
    BATCH_SIZE
)

logger = logging.getLogger(__name__)

# 全局调度器实例
_scheduler: BackgroundScheduler = None
_scheduler_initialized = False

# 检查间隔（秒）
CHECK_INTERVAL = 5


def start_batch_scheduler():
    """启动批次调度器（基于 APScheduler）"""
    global _scheduler, _scheduler_initialized

    if _scheduler and _scheduler.running:
        logger.warning("批次调度器已在运行")
        return

    # 创建调度器
    _scheduler = BackgroundScheduler(
        timezone='Asia/Shanghai',
        daemon=False  # 非守护模式，确保任务能正常完成
    )

    # 添加定时任务
    _scheduler.add_job(
        func=_check_and_load_batches,
        trigger=IntervalTrigger(seconds=CHECK_INTERVAL),
        id='kg_batch_loader',
        name='知识图谱批次加载器',
        replace_existing=True,
        max_instances=1  # 同时只运行一个实例，避免并发冲突
    )

    # 启动调度器
    _scheduler.start()
    logger.info("✅ 批次调度器已启动 (检查间隔: %d秒, 批次大小: %d)", CHECK_INTERVAL, BATCH_SIZE)

    # 将调度器状态写入 Redis（用于监控）
    _update_scheduler_status(running=True)

    # 启动后立即执行一次初始化加载
    if not _scheduler_initialized:
        logger.info("[批次调度器] 执行初始化加载...")
        _initialize_batches()
        _scheduler_initialized = True


def stop_batch_scheduler():
    """停止批次调度器"""
    global _scheduler

    if not _scheduler:
        logger.warning("批次调度器未初始化")
        return

    if not _scheduler.running:
        logger.warning("批次调度器未运行")
        return

    _scheduler.shutdown(wait=False)
    logger.info("批次调度器已停止")


def _initialize_batches():
    """初始化：为每个有主队列任务的 Provider 加载第一批

    在调度器启动时调用一次，确保所有有任务的 Provider 都有活跃批次
    """
    try:
        redis_client = get_redis_client()
        if not redis_client:
            logger.warning("[批次调度器] 初始化: Redis客户端获取失败")
            return

        active_providers = _get_active_providers(redis_client)
        if not active_providers:
            logger.info("[批次调度器] 初始化: 当前没有待处理任务")
            return

        logger.info(f"[批次调度器] 初始化: 发现 {len(active_providers)} 个活跃Provider")
        for provider in active_providers:
            try:
                # 检查是否已有活跃批次
                active_count = get_active_batch_length(provider)
                if active_count > 0:
                    logger.debug(f"[批次调度器] Provider '{provider}' 已有 {active_count} 个活跃任务，跳过初始化")
                    continue

                # 加载第一批
                loaded = load_next_batch(provider, BATCH_SIZE)
                if loaded > 0:
                    logger.info(f"[批次调度器] 初始化: Provider '{provider}' 加载了 {loaded} 个任务")
                else:
                    logger.debug(f"[批次调度器] Provider '{provider}' 没有任务需要加载")

            except Exception as e:
                logger.error(f"[批次调度器] 初始化Provider '{provider}' 失败: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"[批次调度器] 初始化阶段异常: {e}", exc_info=True)


def _check_and_load_batches():
    """检查所有 Provider 的活跃批次，必要时加载下一批

    这是调度器的核心逻辑，会被 APScheduler 定期调用
    """
    try:
        redis_client = get_redis_client()
        if not redis_client:
            logger.debug("[批次调度器] Redis未连接，等待...")
            return

        # 更新调度器状态到 Redis（用于监控）
        _update_scheduler_status(running=True)

        # 获取所有有主队列任务的Provider
        active_providers = _get_active_providers(redis_client)

        if not active_providers:
            # 没有待处理任务
            logger.debug("[批次调度器] 当前没有待处理任务")
            return

        # 检查每个 Provider
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

    except Exception as e:
        logger.error(f"[批次调度器] 检查批次时异常: {e}", exc_info=True)


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
    global _scheduler

    if not _scheduler:
        return {
            'running': False,
            'check_interval': CHECK_INTERVAL,
            'batch_size': BATCH_SIZE,
            'thread_alive': False,
            'scheduler_type': 'APScheduler',
            'jobs': []
        }

    return {
        'running': _scheduler.running,
        'check_interval': CHECK_INTERVAL,
        'batch_size': BATCH_SIZE,
        'thread_alive': _scheduler.running,  # APScheduler 用 running 状态
        'scheduler_type': 'APScheduler',
        'jobs': [
            {
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None
            }
            for job in _scheduler.get_jobs()
        ]
    }


def _update_scheduler_status(running: bool):
    """将调度器状态写入 Redis（用于监控）"""
    import json
    import time

    try:
        redis_client = get_redis_client()
        if not redis_client:
            return

        status_data = {
            'running': running,
            'check_interval': CHECK_INTERVAL,
            'batch_size': BATCH_SIZE,
            'last_run': time.strftime('%Y-%m-%d %H:%M:%S'),
            'scheduler_type': 'APScheduler'
        }

        # 使用 RedisClient 的 set 方法，传入 expire 参数
        redis_client.set(
            'kg:batch_scheduler:info',
            status_data,
            expire=300  # 5分钟过期
        )

    except Exception as e:
        logger.error(f"更新调度器状态到 Redis 失败: {e}")
