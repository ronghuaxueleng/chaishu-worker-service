"""
Redis 分布式锁工具
用于多节点环境下的并发控制
"""
import os
import time
import logging
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger(__name__)


@contextmanager
def redis_distributed_lock(
    redis_client,
    lock_key: str,
    timeout: int = 30,
    blocking: bool = False,
    blocking_timeout: Optional[int] = None
):
    """Redis 分布式锁上下文管理器

    使用 Redis SETNX 实现分布式锁，防止多节点并发冲突

    Args:
        redis_client: Redis 客户端实例
        lock_key: 锁的键名（建议格式: lock:operation_name）
        timeout: 锁超时时间（秒），防止死锁，默认30秒
        blocking: 是否阻塞等待锁，默认 False
        blocking_timeout: 阻塞等待超时时间（秒），None 表示无限等待

    Yields:
        bool: True 表示获取到锁，False 表示未获取到锁

    Example:
        >>> with redis_distributed_lock(redis_client, 'lock:scaling', timeout=30) as acquired:
        ...     if acquired:
        ...         # 执行需要保护的操作
        ...         do_scaling()
        ...     else:
        ...         logger.debug("其他节点正在执行，跳过")

    Example (blocking mode):
        >>> with redis_distributed_lock(
        ...     redis_client,
        ...     'lock:scaling',
        ...     timeout=30,
        ...     blocking=True,
        ...     blocking_timeout=10
        ... ) as acquired:
        ...     if acquired:
        ...         do_scaling()
        ...     else:
        ...         logger.warning("等待锁超时")
    """
    if not redis_client or not redis_client.is_connected():
        logger.warning("Redis 未连接，无法使用分布式锁")
        yield False
        return

    # 生成唯一的锁值（包含进程ID和时间戳）
    lock_value = f"{os.getpid()}_{time.time()}"
    acquired = False
    start_time = time.time()

    try:
        # 尝试获取锁
        while True:
            acquired = redis_client.set(lock_key, lock_value, nx=True, ex=timeout)

            if acquired:
                logger.debug(f"成功获取分布式锁: {lock_key}")
                break

            if not blocking:
                # 非阻塞模式，直接返回
                logger.debug(f"无法获取分布式锁（已被占用）: {lock_key}")
                break

            # 阻塞模式，检查是否超时
            if blocking_timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= blocking_timeout:
                    logger.warning(
                        f"等待分布式锁超时（{blocking_timeout}秒）: {lock_key}"
                    )
                    break

            # 短暂休眠后重试
            time.sleep(0.1)

        yield acquired

    finally:
        if acquired:
            # 释放锁（使用 Lua 脚本确保只释放自己持有的锁）
            lua_script = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            """
            try:
                result = redis_client.eval(lua_script, 1, lock_key, lock_value)
                if result == 1:
                    logger.debug(f"成功释放分布式锁: {lock_key}")
                else:
                    logger.warning(
                        f"释放分布式锁失败（锁可能已过期或被他人持有）: {lock_key}"
                    )
            except Exception as e:
                logger.error(f"释放分布式锁异常: {lock_key}, 错误: {e}")


def acquire_lock(
    redis_client,
    lock_key: str,
    timeout: int = 30,
    blocking: bool = False,
    blocking_timeout: Optional[int] = None
) -> Optional[str]:
    """获取分布式锁（非上下文管理器版本）

    适用于需要手动控制锁释放时机的场景

    Args:
        redis_client: Redis 客户端实例
        lock_key: 锁的键名
        timeout: 锁超时时间（秒）
        blocking: 是否阻塞等待
        blocking_timeout: 阻塞等待超时时间（秒）

    Returns:
        str: 锁的值（用于后续释放），None 表示获取失败

    Example:
        >>> lock_value = acquire_lock(redis_client, 'lock:operation', timeout=30)
        >>> if lock_value:
        ...     try:
        ...         do_operation()
        ...     finally:
        ...         release_lock(redis_client, 'lock:operation', lock_value)
    """
    if not redis_client or not redis_client.is_connected():
        logger.warning("Redis 未连接，无法使用分布式锁")
        return None

    lock_value = f"{os.getpid()}_{time.time()}"
    start_time = time.time()

    while True:
        acquired = redis_client.set(lock_key, lock_value, nx=True, ex=timeout)

        if acquired:
            logger.debug(f"成功获取分布式锁: {lock_key}")
            return lock_value

        if not blocking:
            logger.debug(f"无法获取分布式锁（已被占用）: {lock_key}")
            return None

        if blocking_timeout is not None:
            elapsed = time.time() - start_time
            if elapsed >= blocking_timeout:
                logger.warning(f"等待分布式锁超时（{blocking_timeout}秒）: {lock_key}")
                return None

        time.sleep(0.1)


def release_lock(redis_client, lock_key: str, lock_value: str) -> bool:
    """释放分布式锁

    Args:
        redis_client: Redis 客户端实例
        lock_key: 锁的键名
        lock_value: 获取锁时返回的值

    Returns:
        bool: 是否成功释放
    """
    if not redis_client or not redis_client.is_connected():
        logger.warning("Redis 未连接")
        return False

    lua_script = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """

    try:
        result = redis_client.eval(lua_script, 1, lock_key, lock_value)
        if result == 1:
            logger.debug(f"成功释放分布式锁: {lock_key}")
            return True
        else:
            logger.warning(
                f"释放分布式锁失败（锁可能已过期或被他人持有）: {lock_key}"
            )
            return False
    except Exception as e:
        logger.error(f"释放分布式锁异常: {lock_key}, 错误: {e}")
        return False
