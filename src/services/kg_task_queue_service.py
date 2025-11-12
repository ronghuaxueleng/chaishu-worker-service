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


# 批次管理相关键前缀
MAIN_QUEUE_PREFIX = "kg:main_queue:"      # 主队列（所有待处理任务）
ACTIVE_BATCH_PREFIX = "kg:active_batch:"  # 活跃批次（当前正在处理的一批任务）
BATCH_SIZE = 10  # 每批任务数量





def queue_length(provider: str) -> int:
    """���取指定provider队列长度（新系统：主队列 + 活跃批次）

    注意：已改为使用新的批次队列系统，返回主队列和活跃批次的总长度
    旧的 kg:ai_queue 已废弃
    """
    client = get_redis_client()
    if not client:
        return 0
    try:
        # 使用新队列系统：主队列 + 活跃批次
        main_key = get_main_queue_key(provider)
        active_key = get_active_batch_queue_key(provider)

        main_len = client.llen(main_key)
        active_len = client.llen(active_key)

        return main_len + active_len
    except Exception as e:
        logger.error(f"获取队列长度失败 provider={provider}: {e}")
        return 0




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

        # 🔍 添加调用栈信息，追踪谁在重复调用
        import traceback
        import inspect
        caller_frame = inspect.currentframe().f_back
        caller_info = f"{caller_frame.f_code.co_filename}:{caller_frame.f_lineno} in {caller_frame.f_code.co_name}()"

        client.rpush(key, item)
        logger.info(f"任务 {task_id} 已入队到主队列: {provider} (key={key}) [调用者: {caller_info}]")
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


def is_task_in_any_queue(task_id: int) -> bool:
    """检查任务是否已经在任何队列中（主队列或活跃批次）

    Args:
        task_id: 任务ID

    Returns:
        bool: True 表示任务已在队列中，False 表示不在队列中
    """
    client = get_redis_client()
    if not client:
        return False

    try:
        import json
        task_id_int = int(task_id)

        # 检查所有主队列
        for key in client.keys(f"{MAIN_QUEUE_PREFIX}*"):
            items = client.lrange(key, 0, -1)
            for item in items:
                if isinstance(item, dict):
                    if item.get('task_id') == task_id_int:
                        logger.debug(f"任务 {task_id} 在主队列 {key} 中")
                        return True
                else:
                    try:
                        data = json.loads(item)
                        if data.get('task_id') == task_id_int:
                            logger.debug(f"任务 {task_id} 在主队列 {key} 中")
                            return True
                    except (json.JSONDecodeError, TypeError):
                        continue

        # 检查所有活跃批次
        for key in client.keys(f"{ACTIVE_BATCH_PREFIX}*"):
            items = client.lrange(key, 0, -1)
            for item in items:
                if isinstance(item, dict):
                    if item.get('task_id') == task_id_int:
                        logger.debug(f"任务 {task_id} 在活跃批次 {key} 中")
                        return True
                else:
                    try:
                        data = json.loads(item)
                        if data.get('task_id') == task_id_int:
                            logger.debug(f"任务 {task_id} 在活跃批次 {key} 中")
                            return True
                    except (json.JSONDecodeError, TypeError):
                        continue

        return False

    except Exception as e:
        logger.error(f"检查任务 {task_id} 是否在队列中失败: {e}")
        return False
