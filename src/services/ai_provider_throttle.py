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

MAX_CONSECUTIVE_FAILURES = 3
SUSPEND_SECONDS = 10 * 60  # 10分钟


def _now() -> int:
    return int(time.time())


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
    if not provider_name:
        return
    try:
        if get_redis_client:
            rc = get_redis_client()
            if rc and rc.is_connected():
                key = f"ai:provider:suspend:{provider_name}"
                # 值无所谓，设置过期时间即可
                rc.set(key, {"until": _now() + seconds}, expire=seconds)
                return
    except Exception:
        pass
    _suspended_until[provider_name] = _now() + seconds


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

