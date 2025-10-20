"""
知识图谱任务队列服务（按AI服务商划分队列）
支持批次管理：主队列 -> 活跃批次 -> Worker
"""
from typing import Optional, Dict, Any, List
import json
import logging
import time

from src.utils.redis_client import get_redis_client

logger = logging.getLogger(__name__)


QUEUE_PREFIX = "kg:ai_queue:"
# 批次管理相关键前缀
MAIN_QUEUE_PREFIX = "kg:main_queue:"      # 主队列（所有待处理任务）
ACTIVE_BATCH_PREFIX = "kg:active_batch:"  # 活跃批次（当前正在处理的一批任务）
BATCH_SIZE = 10  # 每批任务数量


def _queue_key(provider: str) -> str:
    provider = (provider or "rules").strip().lower()
    return f"{QUEUE_PREFIX}{provider}"


def enqueue_task(task_id: int, provider: str) -> bool:
    """将任务入队到指定provider队列

    Args:
        task_id: 知识图谱任务ID
        provider: AI服务商名称（或 'rules'）
    """
    client = get_redis_client()
    if not client:
        logger.error("Redis未连接，无法入队")
        return False
    try:
        item = {"task_id": int(task_id), "provider": provider}
        key = _queue_key(provider)
        client.rpush(key, item)
        logger.debug(f"已入队任务: task={task_id}, provider={provider}, key={key}")
        return True
    except Exception as e:
        logger.error(f"任务入队失败 task={task_id}, provider={provider}: {e}")
        return False


def brpop_task(provider: str, timeout: int = 5) -> Optional[Dict[str, Any]]:
    """阻塞弹出一个任务

    Returns:
        dict: {task_id, provider} 或 None
    """
    client = get_redis_client()
    if not client:
        return None
    try:
        key = _queue_key(provider)
        result = client.brpop(key, timeout=timeout)
        if not result:
            return None
        # RedisClient.brpop 已帮我们解json，这里兼容直接返回
        return result
    except Exception as e:
        logger.error(f"阻塞弹出任务失败 provider={provider}: {e}")
        return None


def queue_length(provider: str) -> int:
    """获取指定provider队列长度"""
    client = get_redis_client()
    if not client:
        return 0
    try:
        key = _queue_key(provider)
        return client.llen(key)
    except Exception as e:
        logger.error(f"获取队列长度失败 provider={provider}: {e}")
        return 0


def purge_queue(provider: str) -> int:
    """清空指定provider队列，返回清理的任务数"""
    client = get_redis_client()
    if not client:
        return 0
    try:
        key = _queue_key(provider)
        # 统计长度后删除键
        length = client.llen(key)
        client.delete(key)
        logger.info(f"已清空队列: {key}, 清理 {length} 条")
        return int(length)
    except Exception as e:
        logger.error(f"清空队列失败 provider={provider}: {e}")
        return 0


def rebalance_provider_queue(source_provider: str, target_providers: Optional[List[str]] = None,
                             max_items: Optional[int] = None, strategy: str = 'shortest') -> Dict[str, Any]:
    """将 source_provider 队列中的任务重新分配到 target_providers。

    Args:
        source_provider: 源队列 provider 名称
        target_providers: 目标 provider 列表；为空则不执行
        max_items: 最大迁移数量；None 表示全部
        strategy: 分配策略 'shortest' 或 'round_robin'

    Returns:
        { 'moved': int, 'source_left': int, 'targets': {prov: count} }
    """
    client = get_redis_client()
    if not client:
        return {'moved': 0, 'source_left': 0, 'targets': {}}

    if not target_providers:
        logger.warning("rebalance_provider_queue: 目标provider列表为空，跳过")
        return {'moved': 0, 'source_left': queue_length(source_provider), 'targets': {}}

    # 标准化provider名（去除空格）
    # 注意：_queue_key 会将provider名转为小写用于Redis key，所以这里保持原始大小写即可
    target_providers = [p.strip() for p in target_providers if p]
    target_providers = [p for p in target_providers if p != source_provider and p != 'rules'] or target_providers

    src_key = _queue_key(source_provider)
    moved = 0
    per_target = {p: 0 for p in target_providers}

    # 预取初始队列长度
    try:
        remaining = client.llen(src_key)
    except Exception:
        remaining = 0

    if remaining == 0:
        return {'moved': 0, 'source_left': 0, 'targets': per_target}

    # 准备分配器
    rr_idx = 0
    def _choose_target() -> str:
        nonlocal rr_idx
        if strategy == 'shortest':
            # 动态查询当前长度，选择最短队列
            lens = {p: queue_length(p) for p in target_providers}
            return min(lens.items(), key=lambda kv: kv[1])[0]
        # 默认轮询
        target = target_providers[rr_idx % len(target_providers)]
        rr_idx += 1
        return target

    # 迁移循环
    while remaining > 0 and (max_items is None or moved < max_items):
        try:
            item = client.lpop(src_key)
        except Exception:
            item = None
        if not item:
            break

        # 确保是 dict
        if isinstance(item, dict):
            task = item
        else:
            try:
                task = json.loads(item)
            except Exception:
                # 非法数据，丢弃
                continue

        # 选择目标队列
        target = _choose_target()
        task['provider'] = target
        enqueue_task(task.get('task_id'), target)
        per_target[target] = per_target.get(target, 0) + 1
        moved += 1
        remaining -= 1

    return {'moved': moved, 'source_left': remaining, 'targets': per_target}


# ==================== 批次管理功能 ====================

def enqueue_to_main_queue(task_id: int, provider: str) -> bool:
    """将任务入队到主队列（不是直接入活跃批次）

    Args:
        task_id: 知识图谱任务ID
        provider: AI服务商名称

    Returns:
        bool: 是否成功
    """
    client = get_redis_client()
    if not client:
        logger.error("Redis未连接，无法入队到主队列")
        return False

    try:
        provider_normalized = (provider or "rules").strip().lower()
        key = f"{MAIN_QUEUE_PREFIX}{provider_normalized}"
        item = {"task_id": int(task_id), "provider": provider}

        client.rpush(key, item)
        logger.info(f"任务 {task_id} 已入队到主队列: {provider} (key={key})")
        return True

    except Exception as e:
        logger.error(f"任务入队主队列失败 task={task_id}, provider={provider}: {e}")
        return False


def get_active_batch_queue_key(provider: str) -> str:
    """获取活跃批次队列键"""
    provider_normalized = (provider or "rules").strip().lower()
    return f"{ACTIVE_BATCH_PREFIX}{provider_normalized}"


def get_main_queue_key(provider: str) -> str:
    """获取主队列键"""
    provider_normalized = (provider or "rules").strip().lower()
    return f"{MAIN_QUEUE_PREFIX}{provider_normalized}"


def load_next_batch(provider: str, batch_size: int = BATCH_SIZE) -> int:
    """从主队列加载下一批任务到活跃批次

    Args:
        provider: AI服务商名称
        batch_size: 批次大小

    Returns:
        int: 加载的任务数量
    """
    client = get_redis_client()
    if not client:
        logger.error("Redis未连接，无法加载批次")
        return 0

    try:
        main_queue_key = get_main_queue_key(provider)
        active_batch_key = get_active_batch_queue_key(provider)

        # 检查活跃批次是否为空
        active_count = client.llen(active_batch_key)
        if active_count > 0:
            logger.debug(f"Provider {provider} 活跃批次还有 {active_count} 个任务，跳过加载")
            return 0

        # 从主队列移动任务到活跃批次（使用lpop+lpush组合）
        loaded = 0
        for i in range(batch_size):
            try:
                # 从主队列右侧弹出（实际用lpop从左侧弹）
                item = client.lpop(main_queue_key)
                if item:
                    # 加到活跃批次左侧
                    client.lpush(active_batch_key, item)
                    loaded += 1
                else:
                    break  # 主队列为空
            except Exception as e:
                logger.error(f"移动任务失败: {e}")
                break

        if loaded > 0:
            logger.info(f"[批次管理] Provider {provider} 已从主队列加载 {loaded} 个任务到活跃批次")

            # 设置批次元数据
            batch_meta_key = f"kg:batch_meta:{(provider or 'rules').strip().lower()}"
            client.hmset(batch_meta_key, {
                'loaded_at': time.time(),
                'task_count': loaded,
                'provider': provider
            })
            client.expire(batch_meta_key, 86400)  # 24小时过期

        return loaded

    except Exception as e:
        logger.error(f"加载批次失败 provider={provider}: {e}", exc_info=True)
        return 0


def brpop_from_active_batch(provider: str, timeout: int = 3) -> Optional[Dict[str, Any]]:
    """从活跃批次弹出任务（Worker调用）

    Args:
        provider: AI服务商名称
        timeout: 超时时间（秒）

    Returns:
        dict: {task_id, provider} 或 None
    """
    client = get_redis_client()
    if not client:
        return None

    try:
        active_batch_key = get_active_batch_queue_key(provider)
        result = client.brpop(active_batch_key, timeout=timeout)

        if result:
            logger.debug(f"从活跃批次取到任务: provider={provider}, task={result.get('task_id')}")

        return result

    except Exception as e:
        logger.error(f"从活跃批次弹出任务失败 provider={provider}: {e}")
        return None


def get_main_queue_length(provider: str) -> int:
    """获取主队列长度"""
    client = get_redis_client()
    if not client:
        return 0
    try:
        key = get_main_queue_key(provider)
        return client.llen(key)
    except Exception as e:
        logger.error(f"获取主队列长度失败 provider={provider}: {e}")
        return 0


def get_active_batch_length(provider: str) -> int:
    """获取活跃批次长度"""
    client = get_redis_client()
    if not client:
        return 0
    try:
        key = get_active_batch_queue_key(provider)
        return client.llen(key)
    except Exception as e:
        logger.error(f"获取活跃批次长度失败 provider={provider}: {e}")
        return 0


def purge_main_queue(provider: str) -> int:
    """清空主队列"""
    client = get_redis_client()
    if not client:
        return 0
    try:
        key = get_main_queue_key(provider)
        length = client.llen(key)
        client.delete(key)
        logger.info(f"已清空主队列: {key}, 清理 {length} 条")
        return int(length)
    except Exception as e:
        logger.error(f"清空主队列失败 provider={provider}: {e}")
        return 0


def purge_active_batch(provider: str) -> int:
    """清空活跃批次"""
    client = get_redis_client()
    if not client:
        return 0
    try:
        key = get_active_batch_queue_key(provider)
        length = client.llen(key)
        client.delete(key)
        logger.info(f"已清空活跃批次: {key}, 清理 {length} 条")
        return int(length)
    except Exception as e:
        logger.error(f"清空活跃批次失败 provider={provider}: {e}")
        return 0
