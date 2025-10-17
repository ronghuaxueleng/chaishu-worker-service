"""
知识图谱任务队列服务（按AI服务商划分队列）
"""
from typing import Optional, Dict, Any, List
import json
import logging

from src.utils.redis_client import get_redis_client

logger = logging.getLogger(__name__)


QUEUE_PREFIX = "kg:ai_queue:"


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

    # 规范化provider名为小写
    target_providers = [p.strip().lower() for p in target_providers if p]
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
