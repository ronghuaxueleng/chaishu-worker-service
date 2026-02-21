"""
数据库连接池监控服务

功能：
1. 定期检查连接池状态
2. 连接占用率告警
3. 连接泄漏检测
4. 性能指标收集
"""

import logging
import threading
import time
from typing import Dict, Any, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ConnectionPoolMonitor:
    """连接池监控器"""

    def __init__(self, db_manager, check_interval: int = 60):
        """
        初始化连接池监控器

        Args:
            db_manager: DatabaseManager 实例
            check_interval: 检查间隔（秒），默认 60 秒
        """
        self.db_manager = db_manager
        self.check_interval = check_interval
        self.is_running = False
        self.monitor_thread = None

        # 监控配置
        self.high_usage_threshold = 0.8  # 80% 占用率告警
        self.critical_usage_threshold = 0.95  # 95% 占用率严重告警

        # 统计数据（使用锁保护并发读写）
        self._stats_lock = threading.Lock()
        self.stats_history: List[Dict[str, Any]] = []
        self.max_history_size = 60  # 保留最近 60 条记录（1小时）

        # 告警状态
        self.last_alert_time = None
        self.alert_cooldown = 300  # 5 分钟内不重复告警

    def start(self):
        """启动监控"""
        if self.is_running:
            logger.warning("[连接池监控] 监控已在运行中")
            return

        self.is_running = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="ConnectionPoolMonitor",
            daemon=True
        )
        self.monitor_thread.start()
        logger.info(f"[连接池监控] 启动成功，检查间隔: {self.check_interval}秒")

    def stop(self):
        """停止监控"""
        self.is_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("[连接池监控] 已停止")

    def _monitor_loop(self):
        """监控循环"""
        logger.info("[连接池监控] 监控线程开始运行")

        while self.is_running:
            try:
                self._check_pool_status()
            except Exception as e:
                logger.error(f"[连接池监控] 检查失败: {e}", exc_info=True)

            # 等待下一次检查
            time.sleep(self.check_interval)

        logger.info("[连接池监控] 监控线程已退出")

    def _check_pool_status(self):
        """检查连接池状态"""
        try:
            status = self.db_manager.get_connection_pool_status()

            # 计算使用率
            total = status['total_connections']
            checked_out = status['checked_out_connections']
            usage_ratio = checked_out / total if total > 0 else 0

            # 记录统计
            stats = {
                'timestamp': datetime.now(),
                'pool_size': status['pool_size'],
                'checked_in': status['checked_in_connections'],
                'checked_out': checked_out,
                'overflow': status['overflow_connections'],
                'total': total,
                'usage_ratio': usage_ratio
            }

            self._add_stats(stats)

            # 检查告警条件
            self._check_alerts(stats)

            # 定期输出状态（每 10 分钟）
            if len(self.stats_history) % 10 == 0:
                logger.info(
                    f"[连接池监控] "
                    f"总连接: {total}, "
                    f"使用中: {checked_out}, "
                    f"空闲: {stats['checked_in']}, "
                    f"溢出: {stats['overflow']}, "
                    f"占用率: {usage_ratio:.1%}"
                )

        except Exception as e:
            logger.error(f"[连接池监控] 获取状态失败: {e}")

    def _add_stats(self, stats: Dict[str, Any]):
        """添加统计记录"""
        with self._stats_lock:
            self.stats_history.append(stats)
            # 限制历史记录大小
            if len(self.stats_history) > self.max_history_size:
                self.stats_history.pop(0)

    def _check_alerts(self, stats: Dict[str, Any]):
        """检查告警条件"""
        usage_ratio = stats['usage_ratio']

        # 检查是否在冷却期内
        if self.last_alert_time:
            elapsed = (datetime.now() - self.last_alert_time).total_seconds()
            if elapsed < self.alert_cooldown:
                return

        # 严重告警（95%）
        if usage_ratio >= self.critical_usage_threshold:
            logger.error(
                f"[连接池监控] 🚨 严重告警：连接池占用率 {usage_ratio:.1%} "
                f"({stats['checked_out']}/{stats['total']}) "
                f"- 接近耗尽！可能存在连接泄漏"
            )
            self.last_alert_time = datetime.now()
            self._diagnose_high_usage(stats)

        # 高使用率告警（80%）
        elif usage_ratio >= self.high_usage_threshold:
            logger.warning(
                f"[连接池监控] ⚠️ 连接池占用率较高: {usage_ratio:.1%} "
                f"({stats['checked_out']}/{stats['total']})"
            )
            self.last_alert_time = datetime.now()

    def _diagnose_high_usage(self, stats: Dict[str, Any]):
        """诊断高占用率原因"""
        # 分析历史趋势
        with self._stats_lock:
            recent = list(self.stats_history[-5:])
        if len(recent) >= 5:
            avg_usage = sum(s['usage_ratio'] for s in recent) / len(recent)

            if avg_usage >= self.high_usage_threshold:
                logger.error(
                    f"[连接池监控] 📊 诊断：连接占用持续偏高 "
                    f"(最近5分钟平均: {avg_usage:.1%})，可能原因："
                )
                logger.error("  1. 存在未释放的连接（连接泄漏）")
                logger.error("  2. 并发请求过多，需要增大连接池配置")
                logger.error("  3. 某些查询耗时过长，长时间占用连接")
                logger.error("  4. 建议检查是否所有 session.close() 都被正确调用")

    def get_current_status(self) -> Dict[str, Any]:
        """获取当前连接池状态"""
        try:
            status = self.db_manager.get_connection_pool_status()

            total = status['total_connections']
            checked_out = status['checked_out_connections']
            usage_ratio = checked_out / total if total > 0 else 0

            return {
                'pool_size': status['pool_size'],
                'checked_in_connections': status['checked_in_connections'],
                'checked_out_connections': checked_out,
                'overflow_connections': status['overflow_connections'],
                'total_connections': total,
                'usage_ratio': round(usage_ratio, 4),
                'usage_percentage': f"{usage_ratio:.1%}",
                'status': self._get_health_status(usage_ratio),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"[连接池监控] 获取状态失败: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._stats_lock:
            if not self.stats_history:
                return {
                    'error': '暂无统计数据',
                    'timestamp': datetime.now().isoformat()
                }
            snapshot = list(self.stats_history)
            history_size = len(snapshot)

        recent = snapshot[-10:]  # 最近 10 分钟

        avg_usage = sum(s['usage_ratio'] for s in recent) / len(recent)
        max_usage = max(s['usage_ratio'] for s in recent)
        min_usage = min(s['usage_ratio'] for s in recent)

        avg_checked_out = sum(s['checked_out'] for s in recent) / len(recent)
        max_checked_out = max(s['checked_out'] for s in recent)

        return {
            'monitoring_duration_minutes': history_size,
            'recent_10min': {
                'avg_usage': round(avg_usage, 4),
                'max_usage': round(max_usage, 4),
                'min_usage': round(min_usage, 4),
                'avg_checked_out': round(avg_checked_out, 2),
                'max_checked_out': max_checked_out
            },
            'current': self.get_current_status(),
            'history_size': history_size,
            'timestamp': datetime.now().isoformat()
        }

    def _get_health_status(self, usage_ratio: float) -> str:
        """根据占用率获取健康状态"""
        if usage_ratio >= self.critical_usage_threshold:
            return 'critical'
        elif usage_ratio >= self.high_usage_threshold:
            return 'warning'
        elif usage_ratio >= 0.5:
            return 'normal'
        else:
            return 'healthy'


# 全局监控器实例
_monitor_instance = None


def start_connection_pool_monitor(db_manager, check_interval: int = 60):
    """启动连接池监控（单例模式）"""
    global _monitor_instance

    if _monitor_instance is not None:
        logger.warning("[连接池监控] 监控已启动，跳过重复启动")
        return _monitor_instance

    _monitor_instance = ConnectionPoolMonitor(db_manager, check_interval)
    _monitor_instance.start()
    return _monitor_instance


def stop_connection_pool_monitor():
    """停止连接池监控"""
    global _monitor_instance

    if _monitor_instance:
        _monitor_instance.stop()
        _monitor_instance = None


def get_connection_pool_monitor():
    """获取连接池监控器实例"""
    return _monitor_instance
