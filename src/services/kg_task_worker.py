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

    # 重置 Neo4j 连接（如果使用了连接池）
    try:
        from src.services.knowledge_graph_service import get_kg_service
        # Neo4j driver 会在首次使用时创建，子进程中会自动重新连接
        logger.debug("[KG-Worker] Neo4j 将在首次使用时重新连接")
    except Exception as e:
        logger.warning(f"[KG-Worker] Neo4j 服务导入失败: {e}")

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
                # 无任务，稍作等待
                time.sleep(0.2)
                continue

            task_id = int(item.get('task_id'))
            logger.info(f"[KG-Worker] 取到任务: provider={provider}, task_id={task_id}")

            # 记录当前进程正在处理的任务到 Redis
            try:
                from src.utils.redis_client import get_redis_client
                import socket
                redis_client = get_redis_client()
                if redis_client:
                    # 获取节点名称（优先使用环境变量，否则使用主机名）
                    node_name = os.environ.get('KG_WORKER_NODE_NAME')
                    if not node_name:
                        try:
                            node_name = socket.gethostname()
                        except:
                            node_name = 'unknown'

                    worker_key = f"kg:worker:{os.getpid()}"
                    redis_client.hmset(worker_key, {
                        'provider': provider,
                        'task_id': task_id,
                        'start_time': time.time(),
                        'pid': os.getpid(),
                        'node_name': node_name
                    })
                    redis_client.expire(worker_key, 3600)  # 1小时过期
            except Exception as e:
                logger.warning(f"[KG-Worker] 记录Worker状态到Redis失败: {e}")

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
                # 任务完成后清除 Worker 状态
                try:
                    from src.utils.redis_client import get_redis_client
                    redis_client = get_redis_client()
                    if redis_client:
                        redis_client.delete(f"kg:worker:{os.getpid()}")
                except Exception as e:
                    logger.warning(f"[KG-Worker] 清除Worker状态失败: {e}")

            backoff = 1
        except Exception as e:
            logger.error(f"[KG-Worker] 执行任务异常 provider={provider}: {e}")
            time.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)


def _list_active_providers() -> List[str]:
    """查询激活的 AIProvider 名称列表（小写）"""
    try:
        from src.models.database import db_manager, AIProvider
        session = db_manager.get_session()
        try:
            rows = session.query(AIProvider.name).filter_by(is_active=True).all()
            providers = [name.lower() for (name,) in rows]
            logger.info(f"[KG-Worker] 从数据库查询到 {len(providers)} 个激活的 AIProvider: {providers}")
            return providers
        finally:
            session.close()
    except Exception as e:
        logger.error(f"加载 AIProvider 列表失败: {e}", exc_info=True)
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

    stats = {
        'started': _workers_started,
        'providers': {}
    }

    # 统计进程
    prov_map = {}
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
                                    session = db_manager.get_session()
                                    try:
                                        task = session.query(KnowledgeGraphTask.task_name).filter_by(id=task_id).first()
                                        if task:
                                            task_name = task.task_name
                                    finally:
                                        session.close()
                                except Exception:
                                    pass

                                # 如果没有 node_name 字段，说明是旧版本 Worker，需要重启
                                if not node_name:
                                    node_name = '旧版本(需重启)'

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

        def _loop():
            per = _env_workers_per_provider(default=1)
            logger.info(f"[KG-WorkerGuard] 启动，周期={interval_seconds}s，每Provider进程数={per}")
            loop_count = 0
            while _guard_running:
                try:
                    loop_count += 1
                    # 该函数具备去重能力：仅为缺失的provider拉起进程
                    start_kg_task_workers(per_provider_processes=per)

                    # 每10次循环输出一次健康检查日志
                    if loop_count % 10 == 0:
                        total_alive = len([p for p in _worker_processes if p.is_alive()])
                        logger.debug(f"[KG-WorkerGuard] 健康检查: {total_alive} 个活跃Worker进程")
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
