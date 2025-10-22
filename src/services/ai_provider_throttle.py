"""
AI 服务商失败计数与节流（暂停）工具

需求：
- 若某个 AI 服务商连续 3 次失败，则暂停使用 10 分钟；到期后自动恢复。
- 支持 Redis 存储；若 Redis 不可用，降级为进程内内存存储。
"""
from __future__ import annotations

import logging
import time
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from src.utils.redis_client import get_redis_client  # type: ignore
except Exception:
    get_redis_client = None

# 进程内降级存储
_fail_counts = {}
_suspended_until = {}
_last_request_time = {}  # 记录每个 Provider 最后一次请求时间
_rate_limit_cache = {}   # 缓存 Provider 的频率限制配置

MAX_CONSECUTIVE_FAILURES = 3
SUSPEND_SECONDS = 10 * 60  # 10分钟

# 默认请求频率限制（当数据库不可用时）
DEFAULT_RATE_LIMIT_INTERVAL = 10  # 秒


def _now() -> int:
    return int(time.time())


def get_provider_rate_limit(provider_name: str) -> int:
    """从数据库获取 Provider 的频率限制间隔

    Args:
        provider_name: Provider 名称

    Returns:
        频率限制间隔（秒），0 表示不限制
    """
    if not provider_name:
        return 0

    # 先从缓存读取
    if provider_name in _rate_limit_cache:
        cached_value, cached_time = _rate_limit_cache[provider_name]
        # 缓存 60 秒
        if time.time() - cached_time < 60:
            return cached_value

    # 从数据库读取
    try:
        from src.models.database import AIProvider, db_manager

        with db_manager.get_session() as session:
            provider = session.query(AIProvider).filter(
                AIProvider.name == provider_name
            ).first()

            if provider and hasattr(provider, 'rate_limit_interval'):
                interval = provider.rate_limit_interval or 0
                # 缓存结果
                _rate_limit_cache[provider_name] = (interval, time.time())
                return interval
    except Exception as e:
        logger.warning(f"从数据库读取 Provider '{provider_name}' 频率限制失败: {e}")

    # 降级使用默认值
    return DEFAULT_RATE_LIMIT_INTERVAL


def is_suspended(provider_name: str) -> bool:
    """检查服务商是否处于暂停状态"""
    if not provider_name:
        return False

    # 尝试 Redis
    try:
        if get_redis_client:
            rc = get_redis_client()
            if rc and rc.is_connected():
                key = f"ai:provider:suspend:{provider_name}"
                # 使用存在性 + TTL 即可，不必须读取值
                return rc.exists(key)
    except Exception:
        pass

    # 内存降级
    until = _suspended_until.get(provider_name)
    if until and until > _now():
        return True
    # 过期清除
    if until and until <= _now():
        _suspended_until.pop(provider_name, None)
    return False


def get_failure_count(provider_name: str) -> int:
    """获取当前连续失败次数"""
    if not provider_name:
        return 0
    try:
        if get_redis_client:
            rc = get_redis_client()
            if rc and rc.is_connected():
                key = f"ai:provider:fail:{provider_name}"
                val = rc.get(key)
                return int(val) if isinstance(val, (int, float, str)) and str(val).isdigit() else 0
    except Exception:
        pass
    return int(_fail_counts.get(provider_name, 0))


def reset_failures(provider_name: str) -> None:
    """重置连续失败计数"""
    if not provider_name:
        return
    try:
        if get_redis_client:
            rc = get_redis_client()
            if rc and rc.is_connected():
                key = f"ai:provider:fail:{provider_name}"
                rc.delete(key)
    except Exception:
        pass
    _fail_counts.pop(provider_name, None)


def _set_suspended(provider_name: str, seconds: int = SUSPEND_SECONDS) -> None:
    """暂停Provider并重新分配其活跃任务"""
    if not provider_name:
        return

    # 1. 先设置暂停状态
    try:
        if get_redis_client:
            rc = get_redis_client()
            if rc and rc.is_connected():
                key = f"ai:provider:suspend:{provider_name}"
                # 值无所谓，设置过期时间即可
                rc.set(key, {"until": _now() + seconds}, expire=seconds)
            else:
                raise RuntimeError("redis not connected")
        else:
            raise RuntimeError("redis unavailable")
    except Exception:
        _suspended_until[provider_name] = _now() + seconds

    # 2. 重新分配该Provider的活跃任务到其他Provider
    try:
        from src.services.kg_task_worker import reassign_provider_active_tasks
        reassigned = reassign_provider_active_tasks(provider_name)
        if reassigned > 0:
            logger.info(f"Provider '{provider_name}' 被暂停后，已重新分配 {reassigned} 个活跃任务")
    except Exception as e:
        logger.error(f"重新分配Provider '{provider_name}' 的活跃任务失败: {e}")
        # 不抛出异常，因为暂停操作本身已经成功


def increment_failure(provider_name: str, max_failures: int = MAX_CONSECUTIVE_FAILURES,
                      suspend_seconds: int = SUSPEND_SECONDS) -> Tuple[int, bool]:
    """增加失败计数；达到阈值则暂停。

    Returns:
        (current_count, suspended_now)
    """
    if not provider_name:
        return 0, False

    # 已暂停则不重复计数，但直接告知暂停中
    if is_suspended(provider_name):
        return get_failure_count(provider_name), True

    new_count = 0
    try:
        if get_redis_client:
            rc = get_redis_client()
            if rc and rc.is_connected():
                key = f"ai:provider:fail:{provider_name}"
                val = rc.get(key)
                cur = int(val) if isinstance(val, (int, float, str)) and str(val).isdigit() else 0
                new_count = cur + 1
                rc.set(key, new_count, expire=24 * 60 * 60)  # 24小时内连续计数
            else:
                raise RuntimeError("redis not connected")
        else:
            raise RuntimeError("redis module unavailable")
    except Exception:
        cur = int(_fail_counts.get(provider_name, 0))
        new_count = cur + 1
        _fail_counts[provider_name] = new_count

    if new_count >= max_failures:
        logger.warning(
            f"AI服务商 {provider_name} 连续失败 {new_count} 次，暂停 {suspend_seconds//60} 分钟"
        )
        _set_suspended(provider_name, suspend_seconds)
        reset_failures(provider_name)
        return 0, True

    return new_count, False


def clear_suspension(provider_name: str) -> None:
    """手动清除暂停状态（立即恢复）"""
    if not provider_name:
        return
    try:
        if get_redis_client:
            rc = get_redis_client()
            if rc and rc.is_connected():
                key = f"ai:provider:suspend:{provider_name}"
                rc.delete(key)
    except Exception:
        pass
    _suspended_until.pop(provider_name, None)


def suspend_provider(provider_name: str, duration: int = SUSPEND_SECONDS) -> None:
    """公开接口：暂停指定 Provider

    Args:
        provider_name: Provider 名称
        duration: 暂停时长（秒），默认使用 SUSPEND_SECONDS
    """
    _set_suspended(provider_name, seconds=duration)
    logger.info(f"Provider '{provider_name}' 已被暂停 {duration} 秒")


def should_wait_for_rate_limit(provider_name: str) -> Tuple[bool, float]:
    """检查是否需要因请求频率限制而等待

    从数据库读取 Provider 的频率限制配置

    Args:
        provider_name: Provider 名称

    Returns:
        (should_wait, wait_seconds): 是否需要等待, 需要等待的秒数
    """
    if not provider_name:
        return False, 0.0

    # 从数据库获取频率限制间隔
    min_interval = get_provider_rate_limit(provider_name)

    # 如果间隔为 0，表示不限制
    if min_interval <= 0:
        return False, 0.0

    now = time.time()

    # 尝试从 Redis 读取最后请求时间
    try:
        if get_redis_client:
            rc = get_redis_client()
            if rc and rc.is_connected():
                key = f"ai:provider:last_request:{provider_name}"
                last_time_str = rc.get(key)
                if last_time_str:
                    try:
                        last_time = float(last_time_str)
                        elapsed = now - last_time

                        if elapsed < min_interval:
                            wait_seconds = min_interval - elapsed
                            logger.debug(
                                f"Provider {provider_name} 请求间隔不足: "
                                f"距上次 {elapsed:.1f}秒, 需等待 {wait_seconds:.1f}秒"
                            )
                            return True, wait_seconds
                    except (ValueError, TypeError):
                        pass

                return False, 0.0
    except Exception as e:
        logger.warning(f"从 Redis 读取请求时间失败: {e}")

    # 内存降级
    last_time = _last_request_time.get(provider_name)
    if last_time:
        elapsed = now - last_time

        if elapsed < min_interval:
            wait_seconds = min_interval - elapsed
            logger.debug(
                f"Provider {provider_name} 请求间隔不足: "
                f"距上次 {elapsed:.1f}秒, 需等待 {wait_seconds:.1f}秒"
            )
            return True, wait_seconds

    return False, 0.0


def record_request_time(provider_name: str) -> None:
    """记录 Provider 的请求时间（用于频率限制）

    Args:
        provider_name: Provider 名称
    """
    if not provider_name:
        return

    # 检查是否配置了频率限制
    min_interval = get_provider_rate_limit(provider_name)
    if min_interval <= 0:
        return  # 不限制则不记录

    now = time.time()

    # 尝试写入 Redis
    try:
        if get_redis_client:
            rc = get_redis_client()
            if rc and rc.is_connected():
                key = f"ai:provider:last_request:{provider_name}"
                # 保存24小时，足够长的时间窗口
                rc.set(key, str(now), expire=24 * 60 * 60)
                logger.debug(f"记录 Provider {provider_name} 请求时间: {now}")
                return
    except Exception as e:
        logger.warning(f"写入 Redis 请求时间失败: {e}")

    # 内存降级
    _last_request_time[provider_name] = now
    logger.debug(f"记录 Provider {provider_name} 请求时间(内存): {now}")

