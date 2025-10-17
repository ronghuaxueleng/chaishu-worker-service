"""
Redis客户端管理
用于缓存情节提取过程中的临时数据
"""
import redis
import json
import logging
import os
from typing import Optional, List, Dict, Any
from datetime import timedelta

logger = logging.getLogger(__name__)


class RedisClient:
    """Redis客户端管理类"""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
        db: Optional[int] = None,
        decode_responses: bool = True
    ):
        """
        初始化Redis客户端

        优先级：参数 > 环境变量 > 默认值

        Args:
            host: Redis主机地址
            port: Redis端口
            password: Redis密码
            db: 数据库编号
            decode_responses: 是否自动解码响应为字符串
        """
        # 从环境变量或参数获取配置
        host = host or os.environ.get('REDIS_HOST', 'localhost')
        port = port or int(os.environ.get('REDIS_PORT', '6379'))
        password = password or os.environ.get('REDIS_PASSWORD', '')
        db = db if db is not None else int(os.environ.get('REDIS_DB', '0'))

        try:
            # 创建连接池（供普通操作使用）
            self.pool = redis.ConnectionPool(
                host=host,
                port=port,
                password=password,
                db=db,
                decode_responses=decode_responses,
                socket_connect_timeout=5,
                socket_timeout=30,  # 普通操作 30 秒超时（支持超大章节推送）
                max_connections=50  # 连接池最大连接数
            )

            # 创建 pubsub 专用连接池（使用更长超时）
            self.pubsub_pool = redis.ConnectionPool(
                host=host,
                port=port,
                password=password,
                db=db,
                decode_responses=decode_responses,
                socket_connect_timeout=5,
                socket_timeout=60,  # pubsub 60 秒超时，避免频繁重连
                max_connections=10  # pubsub 连接数较少
            )

            # 创建 BLPOP 专用连接池（使用更长超时，避免队列空闲时超时）
            self.blpop_pool = redis.ConnectionPool(
                host=host,
                port=port,
                password=password,
                db=db,
                decode_responses=decode_responses,
                socket_connect_timeout=5,
                socket_timeout=300,  # BLPOP 300 秒（5分钟）超时，适应长时间空队列
                max_connections=20  # BLPOP 连接数中等
            )

            self.client = redis.Redis(connection_pool=self.pool)

            # 测试连接
            self.client.ping()
            logger.info(f"Redis连接成功: {host}:{port} (db={db}, pool_size=50, pubsub_pool_size=10, blpop_pool_size=20)")
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            self.client = None
            self.pool = None
            self.pubsub_pool = None
            self.blpop_pool = None

    def is_connected(self) -> bool:
        """检查Redis是否连接"""
        if not self.client:
            return False
        try:
            self.client.ping()
            return True
        except:
            return False

    def set(
        self,
        key: str,
        value: Any,
        expire: Optional[int] = None
    ) -> bool:
        """
        设置键值

        Args:
            key: 键
            value: 值（会自动转为JSON）
            expire: 过期时间（秒）

        Returns:
            是否成功
        """
        if not self.is_connected():
            logger.warning("Redis未连接，无法设置键值")
            return False

        try:
            json_value = json.dumps(value, ensure_ascii=False)
            if expire:
                self.client.setex(key, expire, json_value)
            else:
                self.client.set(key, json_value)
            return True
        except Exception as e:
            logger.error(f"Redis设置失败 ({key}): {e}")
            return False

    def get(self, key: str) -> Optional[Any]:
        """
        获取键值

        Args:
            key: 键

        Returns:
            值（自动解析JSON）或None
        """
        if not self.is_connected():
            return None

        try:
            value = self.client.get(key)
            if value is None:
                return None
            return json.loads(value)
        except Exception as e:
            logger.error(f"Redis获取失败 ({key}): {e}")
            return None

    def delete(self, *keys: str) -> int:
        """
        删除键

        Args:
            keys: 要删除的键列表

        Returns:
            删除的键数量
        """
        if not self.is_connected():
            return 0

        try:
            return self.client.delete(*keys)
        except Exception as e:
            logger.error(f"Redis删除失败: {e}")
            return 0

    def exists(self, key: str) -> bool:
        """检查键是否存在"""
        if not self.is_connected():
            return False
        try:
            return self.client.exists(key) > 0
        except:
            return False

    def expire(self, key: str, seconds: int) -> bool:
        """设置键的过期时间"""
        if not self.is_connected():
            return False
        try:
            return self.client.expire(key, seconds)
        except:
            return False

    def keys(self, pattern: str) -> List[str]:
        """查找匹配模式的键"""
        if not self.is_connected():
            return []
        try:
            return self.client.keys(pattern)
        except:
            return []

    # ========== List 操作 ==========

    def lpush(self, key: str, value: Any) -> int:
        """从列表左侧推入值"""
        if not self.is_connected():
            return 0
        try:
            json_value = json.dumps(value, ensure_ascii=False)
            return self.client.lpush(key, json_value)
        except Exception as e:
            logger.error(f"Redis lpush失败 ({key}): {e}")
            return 0

    def rpush_batch(self, key: str, values: list) -> int:
        """批量从列表右侧推入值（使用 pipeline）"""
        if not self.is_connected():
            return 0
        if not values:
            return 0
        try:
            pipeline = self.client.pipeline(transaction=False)
            for value in values:
                json_value = json.dumps(value, ensure_ascii=False)
                pipeline.rpush(key, json_value)
            pipeline.execute()
            return len(values)
        except Exception as e:
            logger.error(f"Redis rpush_batch 失败 ({key}): {e}")
            return 0

    def rpush(self, key: str, value: Any) -> int:
        """从列表右侧推入值"""
        if not self.is_connected():
            return 0
        try:
            json_value = json.dumps(value, ensure_ascii=False)
            return self.client.rpush(key, json_value)
        except Exception as e:
            logger.error(f"Redis rpush失败 ({key}): {e}")
            return 0

    def blpop(self, key: str, timeout: int = 0) -> Optional[Any]:
        """从列表左侧阻塞弹出值（使用专用连接池，带重连机制）

        Args:
            key: 队列键名
            timeout: 超时时间（秒），0 表示永久阻塞

        Returns:
            弹出的值（自动解析JSON）或 None
        """
        if not self.blpop_pool:
            logger.error("BLPOP 连接池未初始化")
            return None

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 使用 BLPOP 专用连接池（300秒超时）
                blpop_client = redis.Redis(connection_pool=self.blpop_pool)
                result = blpop_client.blpop(key, timeout=timeout)

                if result is None:
                    return None

                # result 是 tuple: (key, value)
                key_name, value = result
                return json.loads(value)

            except (redis.exceptions.TimeoutError, redis.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Redis blpop 连接超时 ({key}), 第 {attempt + 1}/{max_retries} 次重试: {e}")
                    # 短暂延迟后重试
                    import time
                    time.sleep(0.5)
                    continue
                else:
                    logger.error(f"Redis blpop 连接超时，已重试 {max_retries} 次 ({key}): {e}")
                    return None

            except json.JSONDecodeError as e:
                logger.error(f"Redis blpop JSON 解析失败 ({key}): {e}")
                return None

            except Exception as e:
                logger.error(f"Redis blpop失败 ({key}): {e}")
                return None

        return None

    def brpop(self, key: str, timeout: int = 0) -> Optional[Any]:
        """从列表右侧阻塞弹出值（timeout=0表示永久阻塞）"""
        if not self.is_connected():
            return None
        try:
            result = self.client.brpop(key, timeout=timeout)
            if result is None:
                return None
            key_name, value = result
            return json.loads(value)
        except Exception as e:
            logger.error(f"Redis brpop失败 ({key}): {e}")
            return None

    def llen(self, key: str) -> int:
        """获取列表长度"""
        if not self.is_connected():
            return 0
        try:
            return self.client.llen(key)
        except Exception as e:
            logger.error(f"Redis llen失败 ({key}): {e}")
            return 0

    def lpop(self, key: str) -> Optional[Any]:
        """从列表左侧弹出一个元素（非阻塞）"""
        if not self.is_connected():
            return None
        try:
            value = self.client.lpop(key)
            if value is None:
                return None
            return json.loads(value)
        except Exception as e:
            logger.error(f"Redis lpop失败 ({key}): {e}")
            return None

    def lrange(self, key: str, start: int = 0, end: int = -1) -> List[Any]:
        """获取列表指定范围的元素"""
        if not self.is_connected():
            return []
        try:
            values = self.client.lrange(key, start, end)
            return [json.loads(v) for v in values]
        except Exception as e:
            logger.error(f"Redis lrange失败 ({key}): {e}")
            return []

    def ltrim(self, key: str, start: int, end: int) -> bool:
        """裁剪列表，只保留指定范围的元素"""
        if not self.is_connected():
            return False
        try:
            self.client.ltrim(key, start, end)
            return True
        except Exception as e:
            logger.error(f"Redis ltrim失败 ({key}): {e}")
            return False

    # ========== Hash 操作 ==========

    def hset(self, key: str, field: str, value: Any) -> int:
        """
        设置Hash字段值

        Args:
            key: Hash键名
            field: 字段名
            value: 值（会自动转为JSON）

        Returns:
            1表示新字段，0表示更新已有字段
        """
        if not self.is_connected():
            return 0
        try:
            json_value = json.dumps(value, ensure_ascii=False)
            return self.client.hset(key, field, json_value)
        except Exception as e:
            logger.error(f"Redis hset失败 ({key}.{field}): {e}")
            return 0

    def hget(self, key: str, field: str) -> Optional[Any]:
        """
        获取Hash字段值

        Args:
            key: Hash键名
            field: 字段名

        Returns:
            值（自动解析JSON）或None
        """
        if not self.is_connected():
            return None
        try:
            value = self.client.hget(key, field)
            if value is None:
                return None
            return json.loads(value)
        except Exception as e:
            logger.error(f"Redis hget失败 ({key}.{field}): {e}")
            return None

    def hgetall(self, key: str) -> Dict[str, Any]:
        """
        获取Hash所有字段和值

        Args:
            key: Hash键名

        Returns:
            字典（值自动解析JSON）
        """
        if not self.is_connected():
            return {}
        try:
            data = self.client.hgetall(key)
            result = {}
            for field, value in data.items():
                try:
                    result[field] = json.loads(value)
                except:
                    result[field] = value
            return result
        except Exception as e:
            logger.error(f"Redis hgetall失败 ({key}): {e}")
            return {}

    def hmset(self, key: str, mapping: Dict[str, Any]) -> bool:
        """
        批量设置Hash字段

        Args:
            key: Hash键名
            mapping: 字段-值字典

        Returns:
            是否成功
        """
        if not self.is_connected():
            return False
        try:
            json_mapping = {
                field: json.dumps(value, ensure_ascii=False)
                for field, value in mapping.items()
            }
            self.client.hset(key, mapping=json_mapping)
            return True
        except Exception as e:
            logger.error(f"Redis hmset失败 ({key}): {e}")
            return False

    def hdel(self, key: str, *fields: str) -> int:
        """
        删除Hash字段

        Args:
            key: Hash键名
            fields: 要删除的字段名列表

        Returns:
            删除的字段数量
        """
        if not self.is_connected():
            return 0
        try:
            return self.client.hdel(key, *fields)
        except Exception as e:
            logger.error(f"Redis hdel失败 ({key}): {e}")
            return 0

    # ========== Set 操作 ==========

    def sadd(self, key: str, *members: Any) -> int:
        """
        向Set添加成员

        Args:
            key: Set键名
            members: 成员列表（会自动转为JSON）

        Returns:
            添加的新成员数量
        """
        if not self.is_connected():
            return 0
        try:
            json_members = [json.dumps(m, ensure_ascii=False) for m in members]
            return self.client.sadd(key, *json_members)
        except Exception as e:
            logger.error(f"Redis sadd失败 ({key}): {e}")
            return 0

    def sismember(self, key: str, member: Any) -> bool:
        """
        检查成员是否在Set中

        Args:
            key: Set键名
            member: 成员（会自动转为JSON）

        Returns:
            是否存在
        """
        if not self.is_connected():
            return False
        try:
            json_member = json.dumps(member, ensure_ascii=False)
            return self.client.sismember(key, json_member)
        except Exception as e:
            logger.error(f"Redis sismember失败 ({key}): {e}")
            return False

    def smembers(self, key: str) -> List[Any]:
        """
        获取Set所有成员

        Args:
            key: Set键名

        Returns:
            成员列表（自动解析JSON）
        """
        if not self.is_connected():
            return []
        try:
            members = self.client.smembers(key)
            return [json.loads(m) for m in members]
        except Exception as e:
            logger.error(f"Redis smembers失败 ({key}): {e}")
            return []

    def scard(self, key: str) -> int:
        """
        获取Set成员数量

        Args:
            key: Set键名

        Returns:
            成员数量
        """
        if not self.is_connected():
            return 0
        try:
            return self.client.scard(key)
        except Exception as e:
            logger.error(f"Redis scard失败 ({key}): {e}")
            return 0

    # ========== Pub/Sub 操作 ==========

    def publish(self, channel: str, message: Any) -> int:
        """发布消息到频道"""
        if not self.is_connected():
            return 0
        try:
            json_message = json.dumps(message, ensure_ascii=False)
            return self.client.publish(channel, json_message)
        except Exception as e:
            logger.error(f"Redis publish失败 ({channel}): {e}")
            return 0

    def subscribe(self, *channels: str):
        """订阅频道（返回 pubsub 对象，使用长超时专用连接池）"""
        if not self.pubsub_pool:
            logger.error("Pubsub 连接池未初始化")
            return None
        try:
            # 使用 pubsub 专用连接池（60 秒超时，避免频繁重连）
            pubsub_client = redis.Redis(connection_pool=self.pubsub_pool)
            pubsub = pubsub_client.pubsub()
            pubsub.subscribe(*channels)
            logger.debug(f"已订阅频道: {channels} (使用 pubsub 专用连接池)")
            return pubsub
        except Exception as e:
            logger.error(f"Redis subscribe失败: {e}")
            return None

    # ========================================

    def close(self):
        """关闭连接和连接池"""
        try:
            if self.client:
                self.client.close()
            if self.pool:
                self.pool.disconnect()
            if self.pubsub_pool:
                self.pubsub_pool.disconnect()
            if self.blpop_pool:
                self.blpop_pool.disconnect()
            logger.info("Redis 连接和所有连接池已关闭")
        except Exception as e:
            logger.error(f"关闭 Redis 连接失败: {e}")


# 全局Redis客户端实例
_redis_client: Optional[RedisClient] = None


def get_redis_client() -> Optional[RedisClient]:
    """获取Redis客户端单例"""
    global _redis_client
    if _redis_client is None:
        _redis_client = RedisClient()
    return _redis_client if _redis_client.is_connected() else None


def close_redis_client():
    """关闭Redis客户端"""
    global _redis_client
    if _redis_client:
        _redis_client.close()
        _redis_client = None
