"""
知识图谱任务 Provider Worker（多进程）
按 AI 服务商划分队列与进程，消费任务并执行现有的构建逻辑。
"""
import logging
import multiprocessing
import threading
import os
import time
from typing import List, Optional

from .kg_task_queue_service import brpop_task

logger = logging.getLogger(__name__)


_workers_started = False
_worker_processes: List[multiprocessing.Process] = []
_guard_thread: Optional[threading.Thread] = None
_guard_running: bool = False
# 暂停日志节流状态：provider -> { 'suspended': bool|None, 'next_log': epoch_seconds }
_suspend_log_state = {}
# 进程管理锁，防止并发创建进程
_worker_lock = threading.Lock()
# 最大进程数限制（保护机制）
MAX_TOTAL_PROCESSES = int(os.environ.get('KG_MAX_TOTAL_PROCESSES', '50'))
MAX_PROCESSES_PER_PROVIDER = int(os.environ.get('KG_MAX_PROCESSES_PER_PROVIDER', '10'))


def kg_task_worker_process(provider: str):
    """Worker 进程函数：消费 provider 队列并执行任务"""
    # 延迟导入，避免主进程初始化时的循环依赖
    from src.services.knowledge_graph_extractor import get_kg_extractor
    from src.models.database import db_manager

    logger.info(f"[KG-Worker] 进程启动: provider={provider}, pid={os.getpid()}")
    # 将当前provider注入到环境变量，供提取阶段覆盖AI选择
    try:
        if provider:
            os.environ['KG_ACTIVE_PROVIDER'] = provider
            logger.info(f"[KG-Worker] 设置运行时Provider: {provider}")
    except Exception as e:
        logger.warning(f"[KG-Worker] 设置Provider覆盖失败: {e}")
    extractor = None
    backoff = 1
    max_backoff = 8

    # 重要：在子进程中重置数据库连接池，避免 fork 后连接复用导致 MySQL Packet sequence 错误
    try:
        db_manager.close_all_connections()
        logger.debug("[KG-Worker] 已重置数据库连接池（子进程）")
    except Exception as e:
        logger.warning(f"[KG-Worker] 重置数据库连接池失败: {e}")

    # 重置 Redis 客户端（避免 fork 后连接复用）
    try:
        from src.utils.redis_client import close_redis_client
        close_redis_client()
        logger.debug("[KG-Worker] 已重置Redis客户端（子进程）")
    except Exception as e:
        logger.warning(f"[KG-Worker] 重置Redis客户端失败: {e}")

    # 重置 Neo4j 连接（如果使用了连接池）
    try:
        from src.services.knowledge_graph_service import get_kg_service
        # Neo4j driver 会在首次使用时创建，子进程中会自动重新连接
        logger.debug("[KG-Worker] Neo4j 将在首次使用时重新连接")
    except Exception as e:
        logger.warning(f"[KG-Worker] Neo4j 服务导入失败: {e}")

    # 在进程启动时就注册到 Redis
    from src.utils.redis_client import get_redis_client
    import socket

    # 获取节点名称
    # - 远程节点（worker-service）：使用环境变量 KG_WORKER_NODE_NAME
    # - 主节点：使用 "主节点-{hostname}" 格式
    node_name = os.environ.get('KG_WORKER_NODE_NAME')
    if node_name:
        logger.debug(f"[KG-Worker] 远程节点 Worker: node_name={node_name}")
    else:
        # 主节点：生成默认节点名
        try:
            hostname = socket.gethostname()
        except:
            hostname = 'unknown'
        node_name = f"主节点-{hostname}"
        logger.debug(f"[KG-Worker] 主节点 Worker: node_name={node_name}")

    def register_worker_to_redis(task_id=None, start_time=None):
        """注册或更新 Worker 状态到 Redis"""
        try:
            redis_client = get_redis_client()
            if not redis_client:
                logger.error(f"[KG-Worker] 无法获取Redis客户端，注册失败: pid={os.getpid()}")
                return False

            worker_key = f"kg:worker:{os.getpid()}"
            worker_data = {
                'provider': provider,
                'pid': os.getpid(),
                'node_name': node_name,
                'last_heartbeat': time.time()
            }
            if task_id is not None:
                worker_data['task_id'] = task_id
                worker_data['start_time'] = start_time or time.time()

            success = redis_client.hmset(worker_key, worker_data)
            if success:
                redis_client.expire(worker_key, 3600)  # 1小时过期（支持长时间任务）
                logger.info(f"[KG-Worker] 注册成功: pid={os.getpid()}, task_id={task_id or '空闲'}, node={node_name}")
                return True
            else:
                logger.error(f"[KG-Worker] hmset失败: pid={os.getpid()}, provider={provider}")
                return False
        except Exception as e:
            logger.error(f"[KG-Worker] 注册Worker状态到Redis失败: {e}", exc_info=True)
            return False

    # 进程启动时立即注册
    register_success = register_worker_to_redis()
    if register_success:
        logger.info(f"[KG-Worker] Worker 启动成功: pid={os.getpid()}, node={node_name}, provider={provider}")
    else:
        logger.error(f"[KG-Worker] Worker 启动失败（Redis注册失败）: pid={os.getpid()}, provider={provider}")

    # 心跳计数器
    heartbeat_counter = 0

    while True:
        try:
            # 若当前Provider被暂停，短暂休眠并跳过取任务，避免将任务取出后再失败
            try:
                from src.services.ai_provider_throttle import is_suspended as _is_suspended
                suspended = bool(provider and provider != 'rules' and _is_suspended(provider))
                st = _suspend_log_state.get(provider or 'unknown', {'suspended': None, 'next_log': 0})
                now = time.time()
                if suspended:
                    # 仅在状态变化或超过节流间隔时输出一条日志（默认120秒）
                    if st.get('suspended') is not True or now >= st.get('next_log', 0):
                        logger.warning(f"[KG-Worker] Provider已暂停，等待恢复: {provider}")
                        st['next_log'] = now + 120
                    st['suspended'] = True
                    _suspend_log_state[provider or 'unknown'] = st

                    # 修复：即使暂停状态下也要定期更新心跳，避免 Redis key 过期
                    heartbeat_counter += 1
                    if heartbeat_counter % 20 == 0:  # 每 100 秒左右更新一次心跳（20次 * 5秒）
                        success = register_worker_to_redis()
                        if success:
                            logger.debug(f"[KG-Worker] 暂停状态心跳更新成功: provider={provider}, pid={os.getpid()}")
                        else:
                            logger.error(f"[KG-Worker] 暂停状态心跳更新失败: provider={provider}, pid={os.getpid()}")

                    time.sleep(5)
                    continue
                else:
                    # 从暂停恢复时输出一次提示
                    if st.get('suspended') is True:
                        logger.info(f"[KG-Worker] Provider已恢复: {provider}")
                    st['suspended'] = False
                    st['next_log'] = 0
                    _suspend_log_state[provider or 'unknown'] = st
            except Exception:
                pass

            item = brpop_task(provider, timeout=3)
            if not item:
                # 无任务，定期更新心跳
                heartbeat_counter += 1
                if heartbeat_counter % 20 == 0:  # 每60秒左右更新一次心跳 (20次 * 3秒)
                    success = register_worker_to_redis()
                    if success:
                        logger.info(f"[KG-Worker] 心跳更新成功: provider={provider}, pid={os.getpid()}, counter={heartbeat_counter}")
                    else:
                        logger.error(f"[KG-Worker] 心跳更新失败: provider={provider}, pid={os.getpid()}")
                time.sleep(0.2)
                continue

            task_id = int(item.get('task_id'))
            logger.info(f"[KG-Worker] 取到任务: provider={provider}, task_id={task_id}")

            # 更新 Worker 状态：记录当前正在处理的任务
            register_worker_to_redis(task_id=task_id, start_time=time.time())

            # 获取提取器实例
            if extractor is None:
                extractor = get_kg_extractor()
                if not extractor:
                    logger.error("知识图谱提取器初始化失败，任务跳过")
                    # 清除 Worker 状态
                    try:
                        from src.utils.redis_client import get_redis_client
                        redis_client = get_redis_client()
                        if redis_client:
                            redis_client.delete(f"kg:worker:{os.getpid()}")
                    except Exception:
                        pass
                    continue

            try:
                # 执行任务（内部含原子 start + 章节事务 + 进度推送）
                extractor.build_knowledge_graph_with_task(task_id)
            finally:
                # 任务完成后，更新 Worker 状态为空闲（不删除记录）
                register_worker_to_redis()  # 不传task_id，表示空闲状态
                heartbeat_counter = 0  # 重置心跳计数器

            backoff = 1
        except Exception as e:
            logger.error(f"[KG-Worker] 执行任务异常 provider={provider}: {e}")
            time.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)


def _list_active_providers() -> List[str]:
    """查询激活的 AIProvider 名称列表（小写）"""
    try:
        from src.models.database import db_manager, AIProvider
        with db_manager.get_session() as session:
            try:
                rows = session.query(AIProvider.name).filter_by(is_active=True).all()
                providers = [name.lower() for (name,) in rows]
                return providers
            except Exception as e:
                session.rollback()
                raise
    except Exception as e:
        logger.error(f"加载 AIProvider 列表失败: {e}")
        return []


def start_kg_task_workers(providers: Optional[List[str]] = None, include_rules: bool = True, per_provider_processes: int = 1):
    """启动 Provider Worker 进程（带严格保护机制）

    Args:
        providers: 指定provider名称；为空则读取激活的AIProvider
        include_rules: 是否包含 'rules' 规则提取进程
        per_provider_processes: 每个provider启动的进程数（默认1）
    """
    global _workers_started, _worker_processes

    # 使用锁防止并发创建进程
    with _worker_lock:
        # 若已启动，仍允许为新增的provider补齐进程（避免整体跳过）
        # 故不直接 return，而是继续走后续逻辑计算需要新起的providers

        if providers is None:
            providers = _list_active_providers()
        providers = [p.strip().lower() for p in (providers or []) if p]
        if include_rules and 'rules' not in providers:
            providers.append('rules')

        if not providers:
            logger.warning("[KG-Worker] 没有可用provider，跳过启动")
            return

        logger.info(f"[KG-Worker] 启动provider进程: {providers}, per={per_provider_processes}")

        # 过滤掉已存在且存活的 provider（按当前进程名判断）
        existing = {}  # provider -> count
        # 同时清理已死亡的进程
        alive_processes = []
        for p in _worker_processes:
            if p.is_alive():
                alive_processes.append(p)
                # 进程名格式: KGTaskWorker-{provider}-{i}
                # provider名称可能包含连字符，所以要从开头去掉"KGTaskWorker-"，从结尾去掉"-{i}"
                name = p.name or ''
                if name.startswith('KGTaskWorker-'):
                    # 去掉前缀
                    provider_part = name[len('KGTaskWorker-'):]
                    # 去掉最后的序号部分 "-1", "-2" 等
                    if '-' in provider_part:
                        provider = '-'.join(provider_part.split('-')[:-1])
                    else:
                        provider = provider_part
                    existing[provider] = existing.get(provider, 0) + 1

        # 更新进程列表，只保留存活的进程
        _worker_processes.clear()
        _worker_processes.extend(alive_processes)

        # 检查总进程数限制
        current_total = len(alive_processes)
        if current_total >= MAX_TOTAL_PROCESSES:
            logger.error(f"[KG-Worker] 已达到最大进程数限制 {MAX_TOTAL_PROCESSES}，拒绝创建新进程")
            return

        logger.info(f"[KG-Worker] 当前活跃进程: {current_total}/{MAX_TOTAL_PROCESSES}")

        # 计算每个 provider 需要启动的进程数
        created_count = 0
        for provider in providers:
            existing_count = existing.get(provider, 0)
            needed_count = per_provider_processes - existing_count

            if needed_count <= 0:
                logger.debug(f"[KG-Worker] provider={provider} 已有 {existing_count} 个进程，无需启动")
                continue

            # 检查 per-provider 限制
            if existing_count >= MAX_PROCESSES_PER_PROVIDER:
                logger.warning(f"[KG-Worker] provider={provider} 已达到单provider最大进程数 {MAX_PROCESSES_PER_PROVIDER}，跳过")
                continue

            # 限制实际创建数量
            allowed_per_provider = min(needed_count, MAX_PROCESSES_PER_PROVIDER - existing_count)
            allowed_total = MAX_TOTAL_PROCESSES - current_total - created_count
            actual_count = min(allowed_per_provider, allowed_total)

            if actual_count <= 0:
                logger.warning(f"[KG-Worker] 总进程数接近上限，跳过 provider={provider}")
                break

            logger.info(f"[KG-Worker] 为 provider={provider} 启动 {actual_count} 个进程（已有 {existing_count}）")

            for i in range(actual_count):
                proc = multiprocessing.Process(
                    target=kg_task_worker_process,
                    args=(provider,),
                    name=f"KGTaskWorker-{provider}-{existing_count + i + 1}",
                    daemon=True
                )
                proc.start()
                _worker_processes.append(proc)
                created_count += 1
                logger.info(f"  - 启动 Worker: provider={provider}, pid={proc.pid}")

        if created_count > 0:
            logger.info(f"[KG-Worker] 本次启动了 {created_count} 个进程，总计 {current_total + created_count} 个")

        _workers_started = True


def get_worker_stats() -> dict:
    """获取 Worker 进程与队列的基本状态

    支持分布式部署：从 Redis 读取所有节点的 Worker 信息

    Returns:
        {
          'started': bool,
          'providers': {
              provider: {
                 'processes': int,
                 'alive': int,
                 'pids': [int,...],
                 'queue_length': int,
                 'active_tasks': [{task_id, pid, start_time, duration}, ...]
              }, ...
          }
        }
    """
    from .kg_task_queue_service import queue_length
    from src.utils.redis_client import get_redis_client
    import psutil

    stats = {
        'started': _workers_started,
        'providers': {}
    }

    prov_map = {}

    # 方案1：统计本地启动的进程（向后兼容）
    for p in _worker_processes:
        name = p.name or ''
        # 名称格式: KGTaskWorker-{provider}-{i}
        provider = 'unknown'
        try:
            if name.startswith('KGTaskWorker-'):
                # 去掉前缀
                provider_part = name[len('KGTaskWorker-'):]
                # 去掉最后的序号部分 "-1", "-2" 等
                if '-' in provider_part:
                    provider = '-'.join(provider_part.split('-')[:-1])
                else:
                    provider = provider_part
        except Exception:
            provider = 'unknown'
        prov = prov_map.setdefault(provider, {'processes': 0, 'alive': 0, 'pids': [], 'active_tasks': []})
        prov['processes'] += 1
        prov['alive'] += 1 if p.is_alive() else 0
        prov['pids'].append(p.pid)

    # 方案2：从 Redis 读取所有节点的 Worker（支持分布式）
    redis_client = None
    stale_worker_keys = []  # 记录需要清理的过期 worker keys

    try:
        redis_client = get_redis_client()
        if redis_client:
            worker_keys = redis_client.keys('kg:worker:*')
            for key in worker_keys:
                try:
                    key_str = key.decode() if isinstance(key, bytes) else key
                    pid = int(key_str.replace('kg:worker:', ''))

                    # 读取 Worker 信息（Hash 类型）
                    worker_info = redis_client.hgetall(key)
                    if not worker_info:
                        continue

                    # 解析 provider 和 node_name
                    provider_raw = worker_info.get(b'provider') if isinstance(list(worker_info.keys())[0], bytes) else worker_info.get('provider')
                    provider = (provider_raw.decode() if isinstance(provider_raw, bytes) else provider_raw) if provider_raw else 'unknown'

                    node_name_raw = worker_info.get(b'node_name') if isinstance(list(worker_info.keys())[0], bytes) else worker_info.get('node_name')
                    node_name = (node_name_raw.decode() if isinstance(node_name_raw, bytes) else node_name_raw) if node_name_raw else None

                    # 健康检查：验证进程是否真实存在
                    is_alive = False

                    # 主节点 Worker：node_name 以"主节点-"开头，使用本地进程检查
                    if node_name and node_name.startswith('主节点-'):
                        try:
                            if psutil.pid_exists(pid):
                                process = psutil.Process(pid)
                                # 检查进程名称中是否包含 python（Worker 进程是 Python 进程）
                                if 'python' in process.name().lower():
                                    is_alive = True
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                    elif node_name:
                        # 远程节点：检查节点心跳是否存在（依赖 Redis TTL 自动清理过期节点）
                        node_key = f"kg:nodes:{node_name}"
                        node_info = redis_client.hgetall(node_key)
                        if node_info:
                            # 节点心跳存在且未过期，信任该节点的所有 Worker 进程
                            is_alive = True
                        else:
                            # 节点心跳不存在（可能已过期或节点已退出），标记 Worker 为过期
                            logger.debug(f"节点心跳不存在，标记 Worker 为过期: node={node_name}, pid={pid}")
                    else:
                        # 兼容旧版本：没有 node_name 的本地 Worker，直接检查进程
                        try:
                            if psutil.pid_exists(pid):
                                process = psutil.Process(pid)
                                # 检查进程名称中是否包含 python（Worker 进程是 Python 进程）
                                if 'python' in process.name().lower():
                                    is_alive = True
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass

                    # 如果进程不存在，标记为过期，稍后清理
                    if not is_alive:
                        stale_worker_keys.append(key_str)
                        logger.debug(f"检测到过期的 Worker 进程: pid={pid}, provider={provider}, node={node_name or '本地'}")
                        continue

                    # 如果这个 PID 不在本地列表中，添加到统计
                    prov = prov_map.setdefault(provider, {'processes': 0, 'alive': 0, 'pids': [], 'active_tasks': []})
                    if pid not in prov['pids']:
                        prov['processes'] += 1
                        prov['alive'] += 1  # 通过健康检查的都认为是活跃的
                        prov['pids'].append(pid)
                except Exception as e:
                    logger.debug(f"解析 Worker Key 失败: {key}, {e}")
                    continue

            # 清理过期的 Worker keys
            if stale_worker_keys:
                try:
                    redis_client.delete(*stale_worker_keys)
                    logger.info(f"清理了 {len(stale_worker_keys)} 个过期的 Worker 进程记录")
                except Exception as e:
                    logger.warning(f"清理过期 Worker keys 失败: {e}")

            # 如果从 Redis 读到了 Worker，设置 started 为 True
            if prov_map:
                stats['started'] = True
    except Exception as e:
        logger.warning(f"从 Redis 读取全局 Worker 失败: {e}")

    # 从 Redis 读取每个进程正在处理的任务和节点信息
    try:
        redis_client = get_redis_client()
        if redis_client:
            for provider, prov_data in prov_map.items():
                for pid in prov_data['pids']:
                    worker_key = f"kg:worker:{pid}"
                    worker_info = redis_client.hgetall(worker_key)
                    if worker_info:
                        task_id = worker_info.get('task_id')
                        start_time = worker_info.get('start_time')
                        node_name = worker_info.get('node_name')  # 读取节点名称

                        # 解码 node_name（如果是 bytes）
                        if isinstance(node_name, bytes):
                            node_name = node_name.decode()

                        if task_id:
                            try:
                                task_id = int(task_id)
                                start_time_float = float(start_time) if start_time else None
                                duration = int(time.time() - start_time_float) if start_time_float else 0

                                # 从数据库获取任务名称
                                task_name = None
                                try:
                                    from src.models.database import db_manager, KnowledgeGraphTask
                                    with db_manager.get_session() as session:
                                        try:
                                            task = session.query(KnowledgeGraphTask.task_name).filter_by(id=task_id).first()
                                            if task:
                                                task_name = task.task_name
                                        except Exception as e:
                                            session.rollback()
                                            raise
                                except Exception:
                                    pass

                                # 处理 node_name 显示
                                # - None 或空字符串：兼容旧版本，显示"主节点"
                                # - 以"主节点-"开头：直接显示
                                # - 其他：正常显示
                                # - 缺少 node_name 字段：真正的旧版本，需要重启
                                if node_name is None or node_name == 'None' or node_name == '':
                                    # 兼容旧版本的主节点 Worker
                                    node_name = '主节点'
                                elif 'node_name' not in worker_info:
                                    # 真正缺少 node_name 字段（旧版本代码）
                                    node_name = '旧版本(需重启)'
                                # 否则直接使用 node_name（包括"主节点-xxx"和远程节点名）

                                prov_data['active_tasks'].append({
                                    'task_id': task_id,
                                    'task_name': task_name,
                                    'pid': pid,
                                    'start_time': start_time_float,
                                    'duration': duration,
                                    'node_name': node_name
                                })
                            except Exception as e:
                                logger.warning(f"解析Worker任务信息失败: {e}")
    except Exception as e:
        logger.warning(f"从Redis读取Worker状态失败: {e}")

    # 融合队列长度
    for provider in list(prov_map.keys()):
        try:
            qlen = queue_length(provider)
        except Exception:
            qlen = 0
        prov_map[provider]['queue_length'] = qlen

    stats['providers'] = prov_map
    return stats


def start_workers_for_providers(providers: List[str], per_provider_processes: int = 1) -> dict:
    """为指定 providers 启动 Worker 进程（即使系统已启动）。

    Returns: { 'started': [prov...], 'skipped': [prov...], 'processes': int }
    """
    if not providers:
        return {'started': [], 'skipped': [], 'processes': 0}

    # 标准化
    providers = [p.strip().lower() for p in providers if p]

    # 当前已有的 provider
    existing = set()
    for p in _worker_processes:
        name = p.name or ''
        parts = name.split('-')
        if len(parts) >= 2:
            existing.add(parts[1])

    to_start = [p for p in providers if p not in existing]
    skipped = [p for p in providers if p in existing]

    started_count = 0
    for provider in to_start:
        for i in range(per_provider_processes):
            proc = multiprocessing.Process(
                target=kg_task_worker_process,
                args=(provider,),
                name=f"KGTaskWorker-{provider}-{i+1}",
                daemon=True
            )
            proc.start()
            _worker_processes.append(proc)
            started_count += 1
            logger.info(f"  - 启动 Worker: provider={provider}, pid={proc.pid}")

    # 标记系统已启动
    global _workers_started
    _workers_started = True

    return {'started': to_start, 'skipped': skipped, 'processes': started_count}


def _env_workers_per_provider(default: int = 1) -> int:
    """读取每个Provider的默认进程数（环境变量 KG_WORKERS_PER_PROVIDER）"""
    try:
        val = os.environ.get('KG_WORKERS_PER_PROVIDER')
        if not val:
            return default
        n = int(str(val).strip())
        return n if n > 0 else default
    except Exception:
        return default


def start_auto_worker_guard(interval_seconds: int = 30):
    """启动后台守护线程，周期性为所有激活的Provider拉起缺失的Worker（带严格保护）

    - 通过环境变量 KG_WORKERS_PER_PROVIDER 控制每个Provider进程数（默认1）
    - 始终包含 'rules' Provider（除非明确不需要，可自行在代码处关掉 include_rules）
    - 防止重复启动守护线程
    - 定期检查并清理僵尸任务（状态为 running 但实际未在执行的任务）
    """
    global _guard_thread, _guard_running

    # 使用锁保护守护线程的启动
    with _worker_lock:
        # 严格检查：如果线程已存在且存活，绝对不启动新线程
        if _guard_thread is not None and _guard_thread.is_alive():
            logger.warning("[KG-WorkerGuard] 守护线程已在运行，拒绝重复启动")
            return

        # 如果线程存在但已死亡，先清理
        if _guard_thread is not None and not _guard_thread.is_alive():
            logger.info("[KG-WorkerGuard] 检测到死亡的守护线程，清理后重新启动")
            _guard_thread = None

        _guard_running = True

        def _check_zombie_tasks():
            """检查并清理僵尸任务（状态为 running 但没有 Worker 在处理）"""
            try:
                from src.models.database import db_manager, KnowledgeGraphTask
                from src.utils.redis_client import get_redis_client

                # 1. 获取所有活跃任务的 task_id
                stats = get_worker_stats()
                active_task_ids = set()
                for prov_stats in stats.get('providers', {}).values():
                    for task in prov_stats.get('active_tasks', []):
                        active_task_ids.add(task.get('task_id'))

                # 2. 查询数据库中状态为 running 的任务
                with db_manager.get_session() as session:
                    try:
                        running_tasks = session.query(KnowledgeGraphTask.id).filter_by(status='running').all()
                        running_task_ids = {t.id for t in running_tasks}

                        # 3. 找出僵尸任务（数据库中 running 但实际未在执行）
                        zombie_task_ids = running_task_ids - active_task_ids

                        if zombie_task_ids:
                            logger.warning(f"[KG-WorkerGuard] 发现 {len(zombie_task_ids)} 个僵尸任务")

                            # 4. 逐个检查僵尸任务并修复状态（限制每次最多100个，避免数据库压力）
                            fixed_count = 0
                            max_fix_per_check = 100
                            sample_zombie_ids = list(zombie_task_ids)[:max_fix_per_check]

                            for task_id in sample_zombie_ids:
                                task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
                                if not task:
                                    continue

                                # 检查任务的完成情况
                                total_chapters = task.total_chapters or 0
                                completed = task.completed_chapters or 0
                                failed = task.failed_chapters or 0

                                # 判断任务应该设置为什么状态
                                if completed >= total_chapters and total_chapters > 0:
                                    # 所有章节已处理完成
                                    task.status = 'completed'
                                    if not task.completed_at:
                                        from datetime import datetime
                                        task.completed_at = datetime.now()
                                    logger.info(f"[KG-WorkerGuard] 僵尸任务 {task_id} ({task.task_name}) 已完成 {completed}/{total_chapters} 章节，修正状态为 completed")
                                elif task.error_message or failed > 0:
                                    # 有错误记录
                                    task.status = 'failed'
                                    logger.info(f"[KG-WorkerGuard] 僵尸任务 {task_id} ({task.task_name}) 有错误（失败章节: {failed}），修正状态为 failed")
                                else:
                                    # 重置为 created，允许重新执行
                                    task.status = 'created'
                                    logger.info(f"[KG-WorkerGuard] 僵尸任务 {task_id} ({task.task_name}) 进度 {completed}/{total_chapters}，重置状态为 created")

                                fixed_count += 1

                            session.commit()
                            remaining = len(zombie_task_ids) - fixed_count
                            if remaining > 0:
                                logger.info(f"[KG-WorkerGuard] 本次修复 {fixed_count} 个僵尸任务，剩余 {remaining} 个将在下次检查时处理")
                            else:
                                logger.info(f"[KG-WorkerGuard] 已修复所有 {fixed_count} 个僵尸任务")
                        else:
                            logger.debug(f"[KG-WorkerGuard] 未发现僵尸任务（{len(running_task_ids)} 个 running 任务都在正常执行）")

                    except Exception as e:
                        session.rollback()
                        raise

            except Exception as e:
                logger.error(f"[KG-WorkerGuard] 检查僵尸任务失败: {e}", exc_info=True)

        def _auto_start_created_tasks():
            """自动启动 created 状态的任务（限流）"""
            try:
                from src.models.database import db_manager, KnowledgeGraphTask
                from src.services.kg_task_queue_service import enqueue_task
                from src.api.kg_task_routes import _choose_provider_for_ai_task

                with db_manager.get_session() as session:
                    try:
                        # 查询 created 状态的任务（限制每次最多启动 20 个）
                        created_tasks = session.query(KnowledgeGraphTask).filter_by(
                            status='created'
                        ).limit(20).all()

                        if not created_tasks:
                            logger.debug("[KG-WorkerGuard] 没有待启动的 created 任务")
                            return

                        logger.info(f"[KG-WorkerGuard] 发现 {len(created_tasks)} 个待启动任务，开始自动入队...")

                        enqueued_count = 0
                        for task in created_tasks:
                            try:
                                # 选择最优 Provider
                                if task.use_ai:
                                    provider = _choose_provider_for_ai_task()
                                else:
                                    provider = 'rules'

                                # 将任务入队
                                if enqueue_task(task.id, provider):
                                    enqueued_count += 1
                                    logger.info(f"[KG-WorkerGuard] 任务 {task.id} ({task.task_name}) 已入队到 {provider}")
                                else:
                                    logger.warning(f"[KG-WorkerGuard] 任务 {task.id} 入队失败")

                            except Exception as e:
                                logger.error(f"[KG-WorkerGuard] 启动任务 {task.id} 失败: {e}")

                        if enqueued_count > 0:
                            logger.info(f"[KG-WorkerGuard] 成功入队 {enqueued_count} 个任务")

                    except Exception as e:
                        session.rollback()
                        raise

            except Exception as e:
                logger.error(f"[KG-WorkerGuard] 自动启动任务失败: {e}", exc_info=True)

        def _loop():
            per = _env_workers_per_provider(default=1)
            logger.info(f"[KG-WorkerGuard] 启动，周期={interval_seconds}s，每Provider进程数={per}")

            # 启动时立即执行一次僵尸任务检查
            logger.info(f"[KG-WorkerGuard] 启动时检查僵尸任务...")
            _check_zombie_tasks()

            # 启动时自动入队 created 任务
            logger.info(f"[KG-WorkerGuard] 启动时检查待启动任务...")
            _auto_start_created_tasks()

            loop_count = 0
            while _guard_running:
                try:
                    loop_count += 1
                    # 该函数具备去重能力：仅为缺失的provider拉起进程
                    start_kg_task_workers(per_provider_processes=per)

                    # 每5次循环（约2.5分钟，假设interval=30s）自动启动 created 任务
                    if loop_count % 5 == 0:
                        _auto_start_created_tasks()

                    # 每10次循环输出一次健康检查日志
                    if loop_count % 10 == 0:
                        total_alive = len([p for p in _worker_processes if p.is_alive()])
                        logger.debug(f"[KG-WorkerGuard] 健康检查: {total_alive} 个活跃Worker进程")

                    # 每20次循环（约10分钟，假设interval=30s）检查一次僵尸任务
                    if loop_count % 20 == 0:
                        logger.info(f"[KG-WorkerGuard] 定期检查僵尸任务...")
                        _check_zombie_tasks()

                except Exception as e:
                    logger.error(f"[KG-WorkerGuard] 保活失败: {e}", exc_info=True)
                # 休眠
                time.sleep(interval_seconds)

            logger.info("[KG-WorkerGuard] 守护线程退出")

        _guard_thread = threading.Thread(target=_loop, name="KG-WorkerGuard", daemon=True)
        _guard_thread.start()
        logger.info(f"[KG-WorkerGuard] 已启动，线程ID={_guard_thread.ident}")


def stop_all_workers(timeout: int = 10):
    """停止所有 Worker 进程和守护线程

    Args:
        timeout: 等待进程退出的超时时间（秒）
    """
    global _guard_running, _guard_thread, _worker_processes

    logger.info("[KG-Worker] 开始停止所有 Worker 进程和守护线程...")

    # 停止守护线程
    if _guard_thread and _guard_thread.is_alive():
        logger.info("[KG-Worker] 停止守护线程...")
        _guard_running = False
        _guard_thread.join(timeout=5)
        if _guard_thread.is_alive():
            logger.warning("[KG-Worker] 守护线程未在5秒内退出")
        else:
            logger.info("[KG-Worker] 守护线程已停止")

    # 终止所有 Worker 进程
    if _worker_processes:
        logger.info(f"[KG-Worker] 终止 {len(_worker_processes)} 个 Worker 进程...")
        for proc in _worker_processes:
            if proc.is_alive():
                proc.terminate()

        # 等待进程退出
        logger.info(f"[KG-Worker] 等待进程退出（最多{timeout}秒）...")
        start_time = time.time()
        for proc in _worker_processes:
            remaining_time = max(0, timeout - (time.time() - start_time))
            proc.join(timeout=remaining_time)
            if proc.is_alive():
                logger.warning(f"[KG-Worker] 进程 {proc.pid} 未在超时时间内退出，强制kill")
                proc.kill()

        _worker_processes.clear()
        logger.info("[KG-Worker] 所有 Worker 进程已停止")

    logger.info("[KG-Worker] 停止完成")
