"""
Redis客户端管理
用于缓存情节提取过程中的临时数据
"""
import redis
import json
import logging
import os
import time
import functools
import threading
from typing import Optional, List, Dict, Any, Callable, TypeVar
from datetime import timedelta, datetime

logger = logging.getLogger(__name__)


# 类型变量用于装饰器
T = TypeVar('T')


# ========================================
# 从配置/环境变量读取重连参数
# ========================================
def _load_reconnect_config():
    """从环境变量或配置文件加载重连配置

    Worker-Service 优先使用环境变量配置
    """
    try:
        # 优先从环境变量读取
        enabled = os.environ.get('REDIS_RECONNECT_ENABLED', 'true').lower() == 'true'
        max_retries = int(os.environ.get('REDIS_MAX_RETRIES', '3'))
        retry_delay = float(os.environ.get('REDIS_RETRY_DELAY', '0.5'))
        health_check_enabled = os.environ.get('REDIS_HEALTH_CHECK_ENABLED', 'true').lower() == 'true'
        health_check_interval = int(os.environ.get('REDIS_HEALTH_CHECK_INTERVAL', '60'))

        return {
            'enabled': enabled,
            'max_retries': max_retries,
            'retry_delay': retry_delay,
            'health_check_enabled': health_check_enabled,
            'health_check_interval': health_check_interval
        }
    except Exception as e:
        logger.warning(f"无法读取重连配置，使用默认值: {e}")
        return {
            'enabled': True,
            'max_retries': 3,
            'retry_delay': 0.5,
            'health_check_enabled': True,
            'health_check_interval': 60
        }


# 模块级配置变量
_RECONNECT_CONFIG = _load_reconnect_config()


def redis_retry(
    max_retries: int = None,
    retry_delay: float = None,
    exceptions: tuple = (redis.exceptions.TimeoutError, redis.exceptions.ConnectionError)
):
    """Redis 操作重试装饰器

    Args:
        max_retries: 最大重试次数（None则使用配置值）
        retry_delay: 重试间隔（秒，None则使用配置值）
        exceptions: 需要重试的异常类型元组

    Returns:
        装饰器函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # 使用传入的参数，如果为None则使用配置值
            actual_max_retries = max_retries if max_retries is not None else _RECONNECT_CONFIG['max_retries']
            actual_retry_delay = retry_delay if retry_delay is not None else _RECONNECT_CONFIG['retry_delay']

            last_exception = None

            for attempt in range(actual_max_retries):
                try:
                    return func(self, *args, **kwargs)

                except exceptions as e:
                    last_exception = e
                    if attempt < actual_max_retries - 1:
                        logger.warning(
                            f"Redis {func.__name__} 操作失败，"
                            f"第 {attempt + 1}/{actual_max_retries} 次重试: {e}"
                        )
                        time.sleep(actual_retry_delay)
                        continue
                    else:
                        logger.error(
                            f"Redis {func.__name__} 操作失败，"
                            f"已重试 {actual_max_retries} 次: {e}"
                        )

            # 所有重试都失败，返回默认值
            if last_exception:
                # 根据函数名返回合适的默认值
                func_name = func.__name__
                if func_name in ['get', 'hget', 'brpop', 'blpop', 'lpop', 'rpop']:
                    return None
                elif func_name in ['llen', 'scard', 'hlen', 'zcard']:
                    return 0
                elif func_name in ['lrange', 'smembers', 'hgetall', 'zrange', 'keys']:
                    return [] if func_name != 'hgetall' else {}
                elif func_name in ['set', 'hset', 'lpush', 'rpush', 'sadd', 'zadd', 'delete', 'expire']:
                    return False
                else:
                    return None

        return wrapper
    return decorator


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

        优先级：参数 > config.json > 默认值

        Args:
            host: Redis主机地址
            port: Redis端口
            password: Redis密码
            db: 数据库编号
            decode_responses: 是否自动解码响应为字符串
        """
        # 从 config.json 读取配置
        try:
            from src.config.config_manager import get_redis_config
            redis_config = get_redis_config()
        except Exception as e:
            logger.warning(f"无法读取 Redis 配置文件: {e}，使用默认配置")
            redis_config = {}

        # 参数优先，然后是配置文件，最后是默认值
        host = host or redis_config.get('host', 'localhost')
        port = port or redis_config.get('port', 6379)
        password = password or redis_config.get('password', '')
        db = db if db is not None else redis_config.get('db', 0)

        # 保存连接参数（用于重连）
        self._host = host
        self._port = port
        self._password = password
        self._db = db
        self._decode_responses = decode_responses

        # 超时和连接池配置（优先级：环境变量 > config.json > 默认值）
        import os

        # 🔧 修复 Kaggle 连接超时：支持从环境变量读取超时配置，默认值改为 15 秒
        socket_connect_timeout = int(os.environ.get('REDIS_SOCKET_CONNECT_TIMEOUT',
                                                      redis_config.get('socket_connect_timeout', 15)))
        socket_timeout = int(os.environ.get('REDIS_SOCKET_TIMEOUT',
                                             redis_config.get('socket_timeout', 60)))
        max_connections = int(os.environ.get('REDIS_MAX_CONNECTIONS',
                                              redis_config.get('max_connections', 50)))
        pubsub_timeout = int(os.environ.get('REDIS_PUBSUB_TIMEOUT',
                                             redis_config.get('pubsub_timeout', 120)))
        pubsub_max_connections = int(os.environ.get('REDIS_PUBSUB_MAX_CONNECTIONS',
                                                     redis_config.get('pubsub_max_connections', 10)))
        blpop_timeout = int(os.environ.get('REDIS_BLPOP_TIMEOUT',
                                            redis_config.get('blpop_timeout', 300)))
        blpop_max_connections = int(os.environ.get('REDIS_BLPOP_MAX_CONNECTIONS',
                                                    redis_config.get('blpop_max_connections', 20)))

        # 保存连接池配置（用于重连）
        self._socket_connect_timeout = socket_connect_timeout
        self._socket_timeout = socket_timeout
        self._max_connections = max_connections
        self._pubsub_timeout = pubsub_timeout
        self._pubsub_max_connections = pubsub_max_connections
        self._blpop_timeout = blpop_timeout
        self._blpop_max_connections = blpop_max_connections

        try:
            # 创建连接池（供普通操作使用）
            self.pool = redis.ConnectionPool(
                host=host,
                port=port,
                password=password,
                db=db,
                decode_responses=decode_responses,
                socket_connect_timeout=socket_connect_timeout,
                socket_timeout=socket_timeout,  # 从配置读取
                max_connections=max_connections  # 从配置读取
            )

            # 创建 pubsub 专用连接池（使用更长超时）
            self.pubsub_pool = redis.ConnectionPool(
                host=host,
                port=port,
                password=password,
                db=db,
                decode_responses=decode_responses,
                socket_connect_timeout=socket_connect_timeout,
                socket_timeout=pubsub_timeout,  # 从配置读取
                max_connections=pubsub_max_connections  # 从配置读取
            )

            # 创建 BLPOP 专用连接池（使用更长超时，避免队列空闲时超时）
            self.blpop_pool = redis.ConnectionPool(
                host=host,
                port=port,
                password=password,
                db=db,
                decode_responses=decode_responses,
                socket_connect_timeout=socket_connect_timeout,
                socket_timeout=blpop_timeout,  # 从配置读取
                max_connections=blpop_max_connections  # 从配置读取
            )

            self.client = redis.Redis(connection_pool=self.pool)

            # 测试连接
            self.client.ping()
            logger.info(f"Redis连接成功: {host}:{port} (db={db}, "
                       f"connect_timeout={socket_connect_timeout}s, "
                       f"socket_timeout={socket_timeout}s, "
                       f"pool_size={max_connections}, "
                       f"pubsub_pool_size={pubsub_max_connections}, "
                       f"blpop_pool_size={blpop_max_connections})")
        except Exception as e:
            # 🔧 改进错误日志：输出详细的连接参数和错误类型
            logger.error(f"Redis连接失败: {type(e).__name__}: {e}")
            logger.error(f"连接参数: host={host}, port={port}, db={db}, "
                        f"connect_timeout={socket_connect_timeout}s, "
                        f"socket_timeout={socket_timeout}s")
            logger.error(f"建议：如果超时，请增加环境变量 REDIS_SOCKET_CONNECT_TIMEOUT（当前={socket_connect_timeout}秒）")
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

    def reconnect(self) -> bool:
        """主动重连 Redis

        Returns:
            重连是否成功
        """
        logger.info("开始重连 Redis...")
        try:
            # 关闭旧连接
            if self.client:
                try:
                    self.client.close()
                except Exception as e:
                    logger.debug(f"关闭旧客户端连接时出错（可忽略）: {e}")

            if self.pool:
                try:
                    self.pool.disconnect()
                except Exception as e:
                    logger.debug(f"断开旧连接池时出错（可忽略）: {e}")

            if self.pubsub_pool:
                try:
                    self.pubsub_pool.disconnect()
                except Exception as e:
                    logger.debug(f"断开旧 pubsub 连接池时出错（可忽略）: {e}")

            if self.blpop_pool:
                try:
                    self.blpop_pool.disconnect()
                except Exception as e:
                    logger.debug(f"断开旧 blpop 连接池时出错（可忽略）: {e}")

            # 重新创建连接池
            self.pool = redis.ConnectionPool(
                host=self._host,
                port=self._port,
                password=self._password,
                db=self._db,
                decode_responses=self._decode_responses,
                socket_connect_timeout=self._socket_connect_timeout,
                socket_timeout=self._socket_timeout,
                max_connections=self._max_connections
            )

            self.pubsub_pool = redis.ConnectionPool(
                host=self._host,
                port=self._port,
                password=self._password,
                db=self._db,
                decode_responses=self._decode_responses,
                socket_connect_timeout=self._socket_connect_timeout,
                socket_timeout=self._pubsub_timeout,
                max_connections=self._pubsub_max_connections
            )

            self.blpop_pool = redis.ConnectionPool(
                host=self._host,
                port=self._port,
                password=self._password,
                db=self._db,
                decode_responses=self._decode_responses,
                socket_connect_timeout=self._socket_connect_timeout,
                socket_timeout=self._blpop_timeout,
                max_connections=self._blpop_max_connections
            )

            # 创建新客户端并测试
            self.client = redis.Redis(connection_pool=self.pool)
            self.client.ping()

            logger.info(f"✅ Redis 重连成功: {self._host}:{self._port} (db={self._db})")
            return True

        except Exception as e:
            logger.error(f"❌ Redis 重连失败: {e}")
            self.client = None
            self.pool = None
            self.pubsub_pool = None
            self.blpop_pool = None
            return False

    @redis_retry()
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
        """从列表右侧阻塞弹出值（带重连机制）

        Args:
            key: 队列键名
            timeout: 超时时间（秒），0 表示永久阻塞

        Returns:
            弹出的值（自动解析JSON）或 None
        """
        if not self.client:
            logger.error("Redis 客户端未初始化")
            return None

        max_retries = _RECONNECT_CONFIG['max_retries']
        retry_delay = _RECONNECT_CONFIG['retry_delay']

        for attempt in range(max_retries):
            try:
                result = self.client.brpop(key, timeout=timeout)

                if result is None:
                    # 正常超时，不是错误
                    return None

                # result 是 tuple: (key, value)
                key_name, value = result
                return json.loads(value)

            except (redis.exceptions.TimeoutError, redis.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Redis brpop 操作失败 ({key}), "
                        f"第 {attempt + 1}/{max_retries} 次重试: {e}"
                    )
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(
                        f"Redis brpop 连接超时，已重试 {max_retries} 次 ({key}): {e}"
                    )
                    return None

            except json.JSONDecodeError as e:
                logger.error(f"Redis brpop JSON 解析失败 ({key}): {e}")
                return None

            except Exception as e:
                logger.error(f"Redis brpop失败 ({key}): {e}")
                return None

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
    """获取Redis客户端单例（支持自动重连）"""
    global _redis_client

    if _redis_client is None:
        logger.info("首次创建 Redis 客户端")
        _redis_client = RedisClient()
        return _redis_client if _redis_client.is_connected() else None

    # 自动重连机制：检测到连接断开时尝试重连
    if not _redis_client.is_connected():
        logger.warning("检测到 Redis 连接断开，尝试重新连接...")
        if _redis_client.reconnect():
            logger.info("✅ Redis 自动重连成功")
            return _redis_client
        else:
            logger.error("❌ Redis 自动重连失败")
            return None

    return _redis_client


def close_redis_client():
    """关闭Redis客户端"""
    global _redis_client
    if _redis_client:
        _redis_client.close()
        _redis_client = None
