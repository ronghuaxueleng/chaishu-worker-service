"""
知识图谱任务管理服务
支持断点续传、进度跟踪、任务控制等功能
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from ..models.database import (
    db_manager, KnowledgeGraphTask, KnowledgeGraphChapterStatus,
    Novel, Chapter, beijing_now
)

logger = logging.getLogger(__name__)


class KnowledgeGraphTaskService:
    """知识图谱任务管理服务"""

    def create_task(self, novel_id: int, task_name: str = None,
                   chapter_ids: List[int] = None, use_ai: bool = True) -> Dict:
        """创建知识图谱构建任务"""
        session = db_manager.get_session()
        try:
            # 获取小说信息
            novel = session.query(Novel).filter_by(id=novel_id).first()
            if not novel:
                raise ValueError(f"小说不存在: {novel_id}")

            # 获取要处理的章节
            if chapter_ids:
                chapters = session.query(Chapter).filter(
                    Chapter.novel_id == novel_id,
                    Chapter.id.in_(chapter_ids)
                ).all()
            else:
                chapters = session.query(Chapter).filter_by(novel_id=novel_id).all()

            if not chapters:
                raise ValueError("没有找到可处理的章节")

            # 生成任务名称
            if not task_name:
                task_name = f"知识图谱构建_{novel.title}_{beijing_now().strftime('%Y%m%d_%H%M%S')}"

            # 创建任务
            task = KnowledgeGraphTask(
                task_name=task_name,
                novel_id=novel_id,
                chapter_ids=chapter_ids,
                use_ai=use_ai,
                total_chapters=len(chapters)
            )
            session.add(task)
            session.flush()  # 获取任务ID

            # 创建章节状态记录
            for chapter in chapters:
                chapter_status = KnowledgeGraphChapterStatus(
                    kg_task_id=task.id,
                    chapter_id=chapter.id,
                    status='pending'
                )
                session.add(chapter_status)

            session.commit()

            logger.info(f"创建知识图谱任务成功: {task.id}, 小说: {novel.title}, 章节数: {len(chapters)}")

            return {
                'task_id': task.id,
                'task_name': task.task_name,
                'novel_id': novel_id,
                'novel_title': novel.title,
                'total_chapters': len(chapters),
                'status': task.status,
                'created_at': task.created_at.isoformat()
            }

        except Exception as e:
            session.rollback()
            logger.error(f"创建知识图谱任务失败: {e}")
            raise
        finally:
            session.close()

    def get_task(self, task_id: int) -> Optional[Dict]:
        """获取任务信息"""
        session = db_manager.get_session()
        try:
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if not task:
                return None

            # 获取章节状态统计
            chapter_status = session.query(KnowledgeGraphChapterStatus).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id
            ).all()

            status_counts = {}
            for status in chapter_status:
                status_counts[status.status] = status_counts.get(status.status, 0) + 1

            return {
                'task_id': task.id,
                'task_name': task.task_name,
                'novel_id': task.novel_id,
                'status': task.status,
                'total_chapters': task.total_chapters,
                'completed_chapters': task.completed_chapters,
                'failed_chapters': task.failed_chapters,
                'skipped_chapters': task.skipped_chapters,
                'current_chapter_id': task.current_chapter_id,
                'total_entities': task.total_entities,
                'total_relations': task.total_relations,
                'error_message': task.error_message,
                'chapter_status_counts': status_counts,
                'created_at': task.created_at.isoformat() if task.created_at else None,
                'started_at': task.started_at.isoformat() if task.started_at else None,
                'completed_at': task.completed_at.isoformat() if task.completed_at else None,
                'paused_at': task.paused_at.isoformat() if task.paused_at else None,
                'updated_at': task.updated_at.isoformat() if task.updated_at else None,
                'use_ai': task.use_ai,
                'retry_count': task.retry_count or 0,
                'auto_retry_enabled': task.auto_retry_enabled,
                'retry_interval_minutes': task.retry_interval_minutes,
                'failed_at': task.failed_at.isoformat() if task.failed_at else None,
                'retry_scheduled_at': task.retry_scheduled_at.isoformat() if task.retry_scheduled_at else None
            }

        except Exception as e:
            logger.error(f"获取任务信息失败: {e}")
            return None
        finally:
            session.close()

    def get_pending_chapters(self, task_id: int) -> List[int]:
        """获取待处理的章节ID列表"""
        session = db_manager.get_session()
        try:
            pending_chapters = session.query(KnowledgeGraphChapterStatus.chapter_id).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id,
                KnowledgeGraphChapterStatus.status == 'pending'
            ).all()

            return [chapter_id for (chapter_id,) in pending_chapters]

        except Exception as e:
            logger.error(f"获取待处理章节失败: {e}")
            return []
        finally:
            session.close()

    def is_task_fully_completed(self, task_id: int) -> bool:
        """检查任务是否真正完成（所有章节都成功处理）"""
        session = db_manager.get_session()
        try:
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if not task:
                return False

            # 获取所有章节状态
            all_chapters = session.query(KnowledgeGraphChapterStatus).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id
            ).all()

            if not all_chapters:
                return False

            # 检查是否所有章节都成功完成
            for chapter_status in all_chapters:
                if chapter_status.status != 'completed':
                    return False

            return True

        except Exception as e:
            logger.error(f"检查任务完成状态失败: {e}")
            return False
        finally:
            session.close()

    def get_task_completion_status(self, task_id: int) -> Dict[str, Any]:
        """获取任务完成状态详情"""
        session = db_manager.get_session()
        try:
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if not task:
                return {'error': '任务不存在'}

            # 统计各状态章节数
            status_counts = {}
            all_chapters = session.query(KnowledgeGraphChapterStatus).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id
            ).all()

            for chapter in all_chapters:
                status = chapter.status
                status_counts[status] = status_counts.get(status, 0) + 1

            total_chapters = len(all_chapters)
            completed_chapters = status_counts.get('completed', 0)
            failed_chapters = status_counts.get('failed', 0)
            pending_chapters = status_counts.get('pending', 0)
            running_chapters = status_counts.get('running', 0)

            # 判断最终状态
            if pending_chapters > 0 or running_chapters > 0:
                final_status = 'running' if running_chapters > 0 else 'pending'
            elif completed_chapters == total_chapters:
                final_status = 'completed'
            elif failed_chapters > 0:
                final_status = 'failed'
            else:
                final_status = 'unknown'

            return {
                'total_chapters': total_chapters,
                'completed_chapters': completed_chapters,
                'failed_chapters': failed_chapters,
                'pending_chapters': pending_chapters,
                'running_chapters': running_chapters,
                'status_counts': status_counts,
                'should_be_completed': completed_chapters == total_chapters and total_chapters > 0,
                'recommended_status': final_status,
                'progress': round((completed_chapters / total_chapters * 100) if total_chapters > 0 else 0, 2)
            }

        except Exception as e:
            logger.error(f"获取任务完成状态详情失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def update_chapter_status(self, task_id: int, chapter_id: int,
                            status: str, error_message: str = None,
                            entities_count: int = None, relations_count: int = None) -> bool:
        """更新章节处理状态"""
        session = db_manager.get_session()
        try:
            # 更新章节状态
            chapter_status = session.query(KnowledgeGraphChapterStatus).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id,
                KnowledgeGraphChapterStatus.chapter_id == chapter_id
            ).first()

            if chapter_status:
                chapter_status.status = status
                chapter_status.updated_at = beijing_now()

                if status == 'running':
                    chapter_status.started_at = beijing_now()
                elif status in ['completed', 'failed', 'skipped']:
                    chapter_status.completed_at = beijing_now()

                if error_message:
                    chapter_status.error_message = error_message

                if entities_count is not None:
                    chapter_status.entities_extracted = entities_count

                if relations_count is not None:
                    chapter_status.relations_extracted = relations_count

            # 更新任务统计（基于内存状态确保一致性）
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if task:
                # 获取所有章节状态到内存中，避免事务中的查询不一致
                all_chapter_statuses = session.query(KnowledgeGraphChapterStatus).filter(
                    KnowledgeGraphChapterStatus.kg_task_id == task_id
                ).all()
                
                # 在内存中统计各状态，包括刚刚更新的章节
                completed_count = 0
                failed_count = 0
                skipped_count = 0
                
                for ch_status in all_chapter_statuses:
                    if ch_status.chapter_id == chapter_id:
                        # 使用当前章节的新状态
                        if status == 'completed':
                            completed_count += 1
                        elif status == 'failed':
                            failed_count += 1
                        elif status == 'skipped':
                            skipped_count += 1
                    else:
                        # 使用其他章节的现有状态
                        if ch_status.status == 'completed':
                            completed_count += 1
                        elif ch_status.status == 'failed':
                            failed_count += 1
                        elif ch_status.status == 'skipped':
                            skipped_count += 1

                task.completed_chapters = completed_count
                task.failed_chapters = failed_count
                task.skipped_chapters = skipped_count

                # 特殊状态处理
                if status == 'failed':
                    if error_message:
                        task.error_message = error_message
                        task.last_error_chapter_id = chapter_id
                elif status == 'running':
                    task.current_chapter_id = chapter_id

                task.updated_at = beijing_now()
                
                logger.info(f"任务{task_id}统计更新(update_chapter_status): 完成={completed_count}, 失败={failed_count}, 跳过={skipped_count}")

            session.commit()
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"更新章节状态失败: {e}")
            return False
        finally:
            session.close()

    def update_task_status(self, task_id: int, status: str,
                          entity_count: int = None, relation_count: int = None) -> bool:
        """更新任务状态"""
        session = db_manager.get_session()
        try:
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if not task:
                return False

            old_status = task.status
            task.status = status
            task.updated_at = beijing_now()

            if status == 'running' and old_status == 'created':
                task.started_at = beijing_now()
            elif status == 'completed':
                task.completed_at = beijing_now()
                # 当任务完成时，确保统计数据正确
                completed = session.query(KnowledgeGraphChapterStatus).filter(
                    KnowledgeGraphChapterStatus.kg_task_id == task_id,
                    KnowledgeGraphChapterStatus.status == 'completed'
                ).count()
                task.completed_chapters = completed
            elif status == 'paused':
                task.paused_at = beijing_now()
            elif status == 'failed':
                # 设置失败时间
                task.failed_at = beijing_now()
                # 如果开启了自动重试，计算下次重试时间
                if task.auto_retry_enabled:
                    from datetime import timedelta
                    retry_interval = task.retry_interval_minutes or 10
                    task.retry_scheduled_at = beijing_now() + timedelta(minutes=retry_interval)
                    logger.info(f"任务 {task_id} 失败，已安排在 {retry_interval} 分钟后重试")

                # 统计需要重试的章节（pending + failed）
                pending_count = session.query(KnowledgeGraphChapterStatus).filter(
                    KnowledgeGraphChapterStatus.kg_task_id == task_id,
                    KnowledgeGraphChapterStatus.status == 'pending'
                ).count()
                failed_count = session.query(KnowledgeGraphChapterStatus).filter(
                    KnowledgeGraphChapterStatus.kg_task_id == task_id,
                    KnowledgeGraphChapterStatus.status == 'failed'
                ).count()
                # 将 pending + failed 作为失败章节数（因为都需要重试）
                task.failed_chapters = pending_count + failed_count
                logger.info(f"任务 {task_id} 失败统计: pending={pending_count}, failed={failed_count}, 总需重试={task.failed_chapters}")

            if entity_count is not None:
                task.total_entities = entity_count
            if relation_count is not None:
                task.total_relations = relation_count

            session.commit()
            logger.info(f"任务状态更新: {task_id} {old_status} -> {status}")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"更新任务状态失败: {e}")
            return False
        finally:
            session.close()

    def try_start_task(self, task_id: int) -> Dict[str, Any]:
        """原子性地尝试启动任务（解决并发竞态条件）"""
        session = db_manager.get_session()
        try:
            # 使用悲观锁防止并发修改
            task = session.query(KnowledgeGraphTask).filter(
                KnowledgeGraphTask.id == task_id
            ).with_for_update().first()

            if not task:
                return {'success': False, 'reason': 'task_not_found', 'message': '任务不存在'}

            # 检查任务是否可以启动
            if task.status == 'running':
                return {'success': False, 'reason': 'already_running', 'message': '任务已在运行中'}
            elif task.status == 'completed':
                return {'success': False, 'reason': 'already_completed', 'message': '任务已完成'}
            elif task.status == 'cancelled':
                return {'success': False, 'reason': 'cancelled', 'message': '任务已取消'}

            # 原子性更新状态
            old_status = task.status
            task.status = 'running'
            task.updated_at = beijing_now()

            if old_status in ['created', 'paused']:
                task.started_at = beijing_now()

            session.commit()

            logger.info(f"任务 {task_id} 原子性启动成功: {old_status} -> running")
            return {
                'success': True,
                'reason': 'started',
                'message': '任务启动成功',
                'old_status': old_status,
                'new_status': 'running'
            }

        except Exception as e:
            session.rollback()
            logger.error(f"原子性启动任务失败: {e}")
            return {
                'success': False,
                'reason': 'database_error',
                'message': f'数据库操作失败: {str(e)}'
            }
        finally:
            session.close()

    def pause_task(self, task_id: int) -> bool:
        """暂停任务"""
        return self.update_task_status(task_id, 'paused')

    def resume_task(self, task_id: int) -> bool:
        """恢复任务"""
        return self.update_task_status(task_id, 'running')

    def cancel_task(self, task_id: int) -> bool:
        """取消任务"""
        return self.update_task_status(task_id, 'cancelled')

    def get_failed_chapters(self, task_id: int) -> List[Dict]:
        """获取失败的章节列表"""
        session = db_manager.get_session()
        try:
            failed_chapters = session.query(
                KnowledgeGraphChapterStatus.chapter_id,
                Chapter.title,
                Chapter.chapter_number,
                KnowledgeGraphChapterStatus.updated_at
            ).join(
                Chapter, KnowledgeGraphChapterStatus.chapter_id == Chapter.id
            ).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id,
                KnowledgeGraphChapterStatus.status == 'failed'
            ).all()

            return [
                {
                    'chapter_id': chapter_id,
                    'title': title,
                    'chapter_number': chapter_number,
                    'failed_at': failed_at.isoformat() if failed_at else None
                }
                for chapter_id, title, chapter_number, failed_at in failed_chapters
            ]

        except Exception as e:
            logger.error(f"获取失败章节失败: {e}")
            return []
        finally:
            session.close()

    def retry_failed_chapters(self, task_id: int, chapter_ids: List[int] = None) -> bool:
        """重试失败的章节或待处理的章节"""
        session = db_manager.get_session()
        try:
            # 先查找失败的章节
            query = session.query(KnowledgeGraphChapterStatus).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id,
                KnowledgeGraphChapterStatus.status == 'failed'
            )

            if chapter_ids:
                query = query.filter(KnowledgeGraphChapterStatus.chapter_id.in_(chapter_ids))

            retry_chapters = query.all()

            # 如果没有失败章节，查找待处理的章节（适用于任务因系统错误失败的情况）
            if not retry_chapters:
                logger.info(f"任务{task_id}没有失败章节，查找待处理章节")
                query = session.query(KnowledgeGraphChapterStatus).filter(
                    KnowledgeGraphChapterStatus.kg_task_id == task_id,
                    KnowledgeGraphChapterStatus.status == 'pending'
                )

                if chapter_ids:
                    query = query.filter(KnowledgeGraphChapterStatus.chapter_id.in_(chapter_ids))

                retry_chapters = query.all()

            # 获取章节详细信息并打印
            chapter_info_list = []
            # 记录有多少章节原本是 failed 状态
            originally_failed_count = sum(1 for c in retry_chapters if c.status == 'failed')

            for chapter_status in retry_chapters:
                # 查询章节信息
                chapter = session.query(Chapter).filter_by(id=chapter_status.chapter_id).first()
                chapter_title = chapter.title if chapter else f"章节{chapter_status.chapter_id}"
                chapter_info_list.append(f"[ID:{chapter_status.chapter_id}, 标题:{chapter_title}]")

                # 确保状态是 pending（如果已经是 pending 就不变，如果是 failed 就重置为 pending）
                chapter_status.status = 'pending'
                chapter_status.updated_at = beijing_now()

                # 批量操作时使用 DEBUG 级别，避免刷屏
                logger.debug(f"准备重试章节: 任务{task_id}, 章节ID: {chapter_status.chapter_id}, 标题: {chapter_title}")

            # 更新任务状态（改为paused，让try_start_task负责原子性地改为running）
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if task and task.status in ['failed', 'paused']:
                # 不直接改成running，而是改成paused，避免与try_start_task冲突
                task.status = 'paused'
                # 只有在有失败章节的情况下才减少失败计数
                if task.failed_chapters > 0 and originally_failed_count > 0:
                    task.failed_chapters = max(0, task.failed_chapters - originally_failed_count)
                task.error_message = None
                task.updated_at = beijing_now()

            session.commit()
            logger.info(f"重试章节: 任务{task_id}, 章节数: {len(retry_chapters)}, 章节列表: {', '.join(chapter_info_list)}")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"重试失败章节失败: {e}")
            return False
        finally:
            session.close()

    def list_tasks(self, novel_id: int = None, status: str = None, limit: int = 50) -> List[Dict]:
        """列出任务"""
        session = db_manager.get_session()
        try:
            query = session.query(KnowledgeGraphTask).join(Novel)

            if novel_id:
                query = query.filter(KnowledgeGraphTask.novel_id == novel_id)
            if status:
                query = query.filter(KnowledgeGraphTask.status == status)

            tasks = query.order_by(KnowledgeGraphTask.created_at.desc()).limit(limit).all()

            result = []
            for task in tasks:
                result.append({
                    'id': task.id,  # 前端期望的字段名
                    'task_id': task.id,  # 保持向后兼容
                    'task_name': task.task_name,
                    'novel_id': task.novel_id,
                    'novel_title': task.novel.title,
                    'status': task.status,
                    'total_chapters': task.total_chapters,
                    'completed_chapters': task.completed_chapters,
                    'failed_chapters': task.failed_chapters,
                    'progress': round((task.completed_chapters / task.total_chapters * 100) if task.total_chapters > 0 else 0, 2),
                    'created_at': task.created_at.isoformat() if task.created_at else None,
                    'updated_at': task.updated_at.isoformat() if task.updated_at else None
                })

            return result

        except Exception as e:
            logger.error(f"列出任务失败: {e}")
            return []
        finally:
            session.close()

    def restart_task(self, task_id: int) -> bool:
        """重启任务（清理Neo4j数据，重置所有章节状态为pending，任务状态为created）"""
        session = db_manager.get_session()
        try:
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if not task:
                return False

            # 只允许重启已完成、失败或取消的任务
            if task.status not in ['completed', 'failed', 'cancelled']:
                raise ValueError(f"无法重启状态为 '{task.status}' 的任务，只能重启已完成、失败或取消的任务")

            logger.info(f"开始重启任务 {task_id}: {task.task_name}")

            # 获取任务包含的所有章节ID
            chapter_ids = session.query(KnowledgeGraphChapterStatus.chapter_id).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id
            ).all()
            chapter_ids = [chapter_id for (chapter_id,) in chapter_ids]

            # 1. 清理Neo4j中的知识图谱数据（精确删除任务相关的节点）
            try:
                from ..services.knowledge_graph_service import get_kg_service
                kg_service = get_kg_service()
                if kg_service:
                    logger.info(f"正在清理任务 {task_id} 在Neo4j中的数据（基于task_id精确删除）")
                    success = kg_service.delete_task_nodes_by_task_id(task_id)
                    if success:
                        logger.info(f"成功清理任务 {task_id} 的Neo4j数据")
                    else:
                        logger.warning(f"清理任务 {task_id} 的Neo4j数据时出现问题")
                else:
                    logger.warning(f"无法获取知识图谱服务，跳过Neo4j数据清理")
            except Exception as e:
                logger.error(f"清理Neo4j数据失败: {e}，继续进行数据库重置")
                # 即使Neo4j清理失败，也继续进行数据库重置

            # 2. 删除关联的情节提取任务
            from sqlalchemy import text
            try:
                # 查找关联的情节提取任务
                plot_tasks = session.execute(
                    text("SELECT id FROM plot_extraction_tasks WHERE kg_task_id = :kg_task_id"),
                    {"kg_task_id": task_id}
                ).fetchall()

                for (plot_task_id,) in plot_tasks:
                    # 删除情节提取日志
                    session.execute(
                        text("DELETE FROM plot_extraction_logs WHERE task_id = :task_id"),
                        {"task_id": plot_task_id}
                    )
                    logger.info(f"删除情节提取任务 {plot_task_id} 的日志")

                # 删除情节提取任务
                deleted_count = session.execute(
                    text("DELETE FROM plot_extraction_tasks WHERE kg_task_id = :kg_task_id"),
                    {"kg_task_id": task_id}
                ).rowcount

                if deleted_count > 0:
                    logger.info(f"重启时删除了 {deleted_count} 个关联的情节提取任务")
            except Exception as e:
                logger.error(f"删除情节提取任务失败: {e}，继续重启任务")
                # 继续重启任务

            # 3. 重置任务状态
            task.status = 'created'
            task.completed_chapters = 0
            task.failed_chapters = 0
            task.total_entities = 0
            task.total_relations = 0
            task.error_message = None
            task.updated_at = beijing_now()

            # 4. 重置所有章节状态为pending
            session.query(KnowledgeGraphChapterStatus).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id
            ).update({
                'status': 'pending',
                'error_message': None,
                'entities_extracted': 0,
                'relations_extracted': 0,
                'updated_at': beijing_now()
            })

            session.commit()

            logger.info(f"成功重启知识图谱任务: {task_id}")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"重启任务失败: {e}")
            return False
        finally:
            session.close()

    def delete_task(self, task_id: int) -> bool:
        """删除任务（只能删除已完成、失败或取消的任务）"""
        session = db_manager.get_session()
        try:
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if not task:
                return False

            if task.status in ['running']:
                raise ValueError("无法删除正在运行的任务")

            # 1. 先删除Neo4j中的知识库数据
            try:
                from .knowledge_graph_service import get_kg_service
                kg_service = get_kg_service()
                if kg_service:
                    logger.info(f"正在删除任务 {task_id} 在Neo4j中的知识库数据")
                    success = kg_service.delete_task_nodes_by_task_id(task_id)
                    if success:
                        logger.info(f"成功删除任务 {task_id} 的Neo4j知识库数据")
                    else:
                        logger.warning(f"删除任务 {task_id} 的Neo4j知识库数据时出现问题")
                else:
                    logger.warning(f"无法获取知识图谱服务，跳过Neo4j数据删除")
            except Exception as e:
                logger.error(f"删除Neo4j知识库数据失败: {e}，继续删除数据库记录")
                # 即使Neo4j删除失败，也继续删除数据库记录

            # 2. 删除关联的情节提取任务
            from sqlalchemy import text
            try:
                # 查找关联的情节提取任务
                plot_tasks = session.execute(
                    text("SELECT id FROM plot_extraction_tasks WHERE kg_task_id = :kg_task_id"),
                    {"kg_task_id": task_id}
                ).fetchall()

                for (plot_task_id,) in plot_tasks:
                    # 删除情节提取日志
                    session.execute(
                        text("DELETE FROM plot_extraction_logs WHERE task_id = :task_id"),
                        {"task_id": plot_task_id}
                    )
                    logger.info(f"删除情节提取任务 {plot_task_id} 的日志")

                # 删除情节提取任务
                deleted_count = session.execute(
                    text("DELETE FROM plot_extraction_tasks WHERE kg_task_id = :kg_task_id"),
                    {"kg_task_id": task_id}
                ).rowcount

                if deleted_count > 0:
                    logger.info(f"删除了 {deleted_count} 个关联的情节提取任务")
            except Exception as e:
                logger.error(f"删除情节提取任务失败: {e}，继续删除主任务")
                # 继续删除主任务

            # 3. 删除章节状态记录
            session.query(KnowledgeGraphChapterStatus).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id
            ).delete()

            # 4. 删除任务
            session.delete(task)
            session.commit()

            logger.info(f"删除知识图谱任务: {task_id}")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"删除任务失败: {e}")
            return False
        finally:
            session.close()

    def recover_interrupted_tasks(self) -> Dict[str, int]:
        """系统启动时恢复中断的任务状态"""
        session = db_manager.get_session()
        try:
            # 查找状态为'running'的孤立章节
            orphaned_chapters = session.query(KnowledgeGraphChapterStatus).filter(
                KnowledgeGraphChapterStatus.status == 'running'
            ).all()

            if not orphaned_chapters:
                logger.info("没有发现需要恢复的章节状态")
                return {'recovered_chapters': 0, 'recovered_tasks': 0}

            # 按任务分组
            task_chapters = {}
            for chapter_status in orphaned_chapters:
                task_id = chapter_status.kg_task_id
                if task_id not in task_chapters:
                    task_chapters[task_id] = []
                task_chapters[task_id].append(chapter_status)

            recovered_chapters = 0
            recovered_tasks = 0

            # 恢复每个任务的章节状态
            for task_id, chapter_statuses in task_chapters.items():
                try:
                    # 获取任务信息
                    task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
                    if not task:
                        logger.warning(f"任务 {task_id} 不存在，跳过恢复")
                        continue

                    # 重置章节状态为pending
                    for chapter_status in chapter_statuses:
                        chapter_status.status = 'pending'
                        chapter_status.updated_at = beijing_now()
                        chapter_status.started_at = None
                        recovered_chapters += 1

                    # 更新任务状态
                    if task.status == 'running':
                        task.status = 'created'  # 重置为初始状态，等待重新启动
                        task.current_chapter_id = None
                        task.updated_at = beijing_now()
                        recovered_tasks += 1

                    logger.info(f"恢复任务 {task_id}: {len(chapter_statuses)} 个章节状态")

                except Exception as e:
                    logger.error(f"恢复任务 {task_id} 失败: {e}")
                    continue

            session.commit()

            logger.info(f"状态恢复完成: 恢复 {recovered_tasks} 个任务, {recovered_chapters} 个章节")
            return {
                'recovered_chapters': recovered_chapters,
                'recovered_tasks': recovered_tasks
            }

        except Exception as e:
            session.rollback()
            logger.error(f"状态恢复失败: {e}")
            return {'recovered_chapters': 0, 'recovered_tasks': 0}
        finally:
            session.close()

    def process_chapter_with_transaction_safety(self, task_id: int, chapter_id: int,
                                               entities_count: int, relations_count: int,
                                               neo4j_success: bool = True) -> bool:
        """
        安全地处理章节完成（分布式事务补偿机制）

        Args:
            task_id: 任务ID
            chapter_id: 章节ID
            entities_count: 实体数量
            relations_count: 关系数量
            neo4j_success: Neo4j操作是否成功

        Returns:
            bool: 处理是否成功
        """
        session = db_manager.get_session()
        try:
            # 1. 首先尝试更新数据库状态
            chapter_status = session.query(KnowledgeGraphChapterStatus).filter(
                KnowledgeGraphChapterStatus.kg_task_id == task_id,
                KnowledgeGraphChapterStatus.chapter_id == chapter_id
            ).first()

            if not chapter_status:
                logger.error(f"章节状态记录不存在: 任务{task_id}, 章节{chapter_id}")
                return False

            # 保存原始状态用于回滚
            original_status = chapter_status.status

            if neo4j_success:
                # Neo4j成功，标记章节完成
                chapter_status.status = 'completed'
                chapter_status.entities_extracted = entities_count
                chapter_status.relations_extracted = relations_count
                chapter_status.completed_at = beijing_now()
                chapter_status.error_message = None
            else:
                # Neo4j失败，标记章节失败
                chapter_status.status = 'failed'
                chapter_status.error_message = "知识图谱数据创建失败"
                chapter_status.completed_at = beijing_now()

            chapter_status.updated_at = beijing_now()

            # 2. 更新任务统计（基于内存状态确保一致性）
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if task:
                # 获取所有章节状态到内存中，避免事务中的查询不一致
                all_chapter_statuses = session.query(KnowledgeGraphChapterStatus).filter(
                    KnowledgeGraphChapterStatus.kg_task_id == task_id
                ).all()
                
                # 在内存中统计各状态，包括刚刚更新的章节
                completed_count = 0
                failed_count = 0
                
                for ch_status in all_chapter_statuses:
                    if ch_status.chapter_id == chapter_id:
                        # 使用当前章节的新状态
                        if neo4j_success:
                            completed_count += 1
                        else:
                            failed_count += 1
                    else:
                        # 使用其他章节的现有状态
                        if ch_status.status == 'completed':
                            completed_count += 1
                        elif ch_status.status == 'failed':
                            failed_count += 1

                task.completed_chapters = completed_count
                task.failed_chapters = failed_count
                task.updated_at = beijing_now()

                # 如果是完成状态，累加实体和关系统计
                if neo4j_success:
                    task.total_entities = (task.total_entities or 0) + entities_count
                    task.total_relations = (task.total_relations or 0) + relations_count
                    
                logger.info(f"任务{task_id}统计更新: 完成章节={completed_count}, 失败章节={failed_count}")

            # 3. 提交数据库事务
            session.commit()

            logger.info(f"章节处理事务完成: 任务{task_id}, 章节{chapter_id}, "
                       f"状态: {original_status} -> {chapter_status.status}, "
                       f"实体: {entities_count}, 关系: {relations_count}")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"章节处理事务失败: 任务{task_id}, 章节{chapter_id}, 错误: {e}")

            # 如果数据库操作失败，但Neo4j已成功，需要记录不一致状态
            if neo4j_success:
                logger.warning(f"数据不一致: Neo4j成功但数据库更新失败，任务{task_id}, 章节{chapter_id}")
                # 可以在这里添加数据修复机制或告警

            return False
        finally:
            session.close()


    def toggle_auto_retry(self, task_id: int, enabled: bool, retry_interval_minutes: int = 10) -> bool:
        """开启或关闭任务的自动重试功能

        Args:
            task_id: 任务ID
            enabled: 是否启用自动重试
            retry_interval_minutes: 重试间隔(分钟)，默认10分钟
        """
        session = db_manager.get_session()
        try:
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if not task:
                logger.warning(f"任务不存在: {task_id}")
                return False

            task.auto_retry_enabled = enabled
            task.retry_interval_minutes = retry_interval_minutes
            task.updated_at = beijing_now()

            # 如果启用自动重试且当前任务是失败状态，立即计划重试
            if enabled and task.status == 'failed':
                from datetime import timedelta
                task.retry_scheduled_at = beijing_now() + timedelta(minutes=retry_interval_minutes)
                logger.info(f"任务 {task_id} 启用自动重试，计划在 {task.retry_scheduled_at} ({retry_interval_minutes}分钟后) 重试")

            session.commit()
            logger.info(f"任务 {task_id} 自动重试已{'启用' if enabled else '禁用'}，重试间隔: {retry_interval_minutes}分钟")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"切换自动重试状态失败: {e}")
            return False
        finally:
            session.close()


    def get_tasks_pending_retry(self) -> List[Dict]:
        """获取需要重试的任务列表"""
        session = db_manager.get_session()
        try:
            now = beijing_now()
            tasks = session.query(KnowledgeGraphTask).filter(
                KnowledgeGraphTask.status == 'failed',
                KnowledgeGraphTask.auto_retry_enabled == True,
                KnowledgeGraphTask.retry_scheduled_at <= now
            ).all()

            result = []
            for task in tasks:
                result.append({
                    'id': task.id,
                    'task_name': task.task_name,
                    'novel_id': task.novel_id,
                    'failed_at': task.failed_at.isoformat() if task.failed_at else None,
                    'retry_scheduled_at': task.retry_scheduled_at.isoformat() if task.retry_scheduled_at else None,
                    'retry_count': task.retry_count
                })

            return result

        except Exception as e:
            logger.error(f"获取待重试任务失败: {e}")
            return []
        finally:
            session.close()


    def mark_task_for_retry(self, task_id: int) -> bool:
        """标记任务失败并设置重试时间"""
        session = db_manager.get_session()
        try:
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if not task:
                return False

            from datetime import timedelta
            task.status = 'failed'
            task.failed_at = beijing_now()

            # 只有启用了自动重试才设置重试时间
            if task.auto_retry_enabled:
                retry_minutes = task.retry_interval_minutes or 10  # 默认10分钟
                task.retry_scheduled_at = beijing_now() + timedelta(minutes=retry_minutes)
                logger.info(f"任务 {task_id} 失败，已计划在 {task.retry_scheduled_at} ({retry_minutes}分钟后) 自动重试")

            task.updated_at = beijing_now()

            session.commit()
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"标记任务重试失败: {e}")
            return False
        finally:
            session.close()


    def execute_retry(self, task_id: int) -> bool:
        """执行任务重试"""
        session = db_manager.get_session()
        try:
            task = session.query(KnowledgeGraphTask).filter_by(id=task_id).first()
            if not task:
                logger.warning(f"重试失败: 任务 {task_id} 不存在")
                return False

            # 增加重试次数
            task.retry_count = (task.retry_count or 0) + 1
            task.retry_scheduled_at = None  # 清除计划重试时间
            task.updated_at = beijing_now()

            session.commit()

            logger.info(f"开始执行任务 {task_id} 的第 {task.retry_count} 次重试")

            # 调用重试失败章节的方法
            return self.retry_failed_chapters(task_id)

        except Exception as e:
            session.rollback()
            logger.error(f"执行重试失败: {e}")
            return False
        finally:
            session.close()


# 全局任务服务实例
kg_task_service = KnowledgeGraphTaskService()