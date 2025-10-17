from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc, case
from typing import List, Optional, Dict, Any
from datetime import datetime
import os
import contextlib
import logging

from src.models.database import db_manager, Novel, Chapter, PromptTemplate, AIProvider, Analysis, AnalysisTask, Workflow, WorkflowStep, WorkflowExecution, WorkflowStepExecution, TaskChapterStatus, KnowledgeGraphChapterStatus, KnowledgeGraphTask, beijing_now

logger = logging.getLogger(__name__)

class DatabaseService:
    def __init__(self):
        self.db_manager = db_manager
        if self.db_manager is None:
            logger.error("数据库管理器初始化失败，db_manager为None")
            raise RuntimeError("数据库管理器初始化失败")
    
    @contextlib.contextmanager
    def get_session(self):
        """获取数据库会话的上下文管理器"""
        if self.db_manager is None:
            raise RuntimeError("数据库管理器未初始化")
        session = self.db_manager.get_session()
        if session is None:
            raise RuntimeError("无法获取数据库会话")
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def get_connection_pool_status(self):
        """获取连接池状态"""
        return self.db_manager.get_connection_pool_status()
    
    def reload_connections(self):
        """重新加载数据库连接"""
        try:
            logger.info("开始重新加载数据库连接...")
            # 关闭现有连接池
            if hasattr(self.db_manager, 'close_pool'):
                self.db_manager.close_pool()
            
            # 重新初始化数据库管理器
            from src.models.database import DatabaseManager
            self.db_manager = DatabaseManager()
            # 只创建表，不重新初始化默认数据
            self.db_manager.create_tables()
            
            logger.info("数据库连接重新加载完成")
            return True
        except Exception as e:
            logger.error(f"重新加载数据库连接失败: {str(e)}")
            raise e
    
    # Novel operations
    def create_novel(self, title: str, author: str = None, description: str = None,
                    file_path: str = None, file_size: int = None) -> Novel:
        with self.get_session() as session:
            novel = Novel(
                title=title,
                author=author,
                description=description,
                file_path=file_path,
                file_size=file_size
            )
            session.add(novel)
            session.commit()
            session.refresh(novel)

            # 清除小说标识符缓存
            self.invalidate_novel_identifiers_cache()

            return novel
    
    def update_novel_word_count(self, novel_id: int, total_word_count: int) -> bool:
        """更新小说的总字数"""
        with self.get_session() as session:
            try:
                novel = session.query(Novel).filter(Novel.id == novel_id).first()
                if novel:
                    novel.total_word_count = total_word_count
                    session.commit()
                    return True
                return False
            except Exception as e:
                logger.error(f"更新小说字数失败: {e}")
                session.rollback()
                return False

    def update_novel_tags(self, novel_id: int, tags: List[str]) -> bool:
        """更新小说的标签"""
        with self.get_session() as session:
            try:
                novel = session.query(Novel).filter(Novel.id == novel_id).first()
                if novel:
                    novel.tags = tags
                    novel.updated_at = beijing_now()
                    session.commit()
                    return True
                return False
            except Exception as e:
                logger.error(f"更新小说标签失败: {e}")
                session.rollback()
                return False

    def get_novel_by_id(self, novel_id: int) -> Optional[Novel]:
        with self.get_session() as session:
            return session.query(Novel).filter(Novel.id == novel_id).first()

    def get_all_novel_identifiers(self, use_cache: bool = True) -> Dict[str, int]:
        """获取所有小说的标识符（用于批量导入时快速检查重复）

        Args:
            use_cache: 是否使用Redis缓存

        Returns:
            字典，key为标识符，value为小说ID
            标识符格式：
            - 如果有file_path: path:文件路径
            - 如果有author: title_author:标题|作者
            - 否则: title:标题
        """
        # 尝试从Redis缓存获取
        if use_cache:
            try:
                from src.utils.redis_client import get_redis_client
                redis_client = get_redis_client()
                if redis_client:
                    cache_key = "novel_identifiers"
                    cached_data = redis_client.get(cache_key)
                    if cached_data:
                        logger.info(f"从Redis缓存加载小说标识符: {len(cached_data)} 条")
                        return cached_data
            except Exception as e:
                logger.warning(f"从Redis获取缓存失败: {e}，将从数据库查询")

        # 从数据库查询
        with self.get_session() as session:
            novels = session.query(Novel).all()
            identifiers = {}

            for novel in novels:
                # 优先使用文件路径作为唯一标识
                if novel.file_path:
                    identifiers[f"path:{novel.file_path}"] = novel.id

                # 使用标题+作者作为标识
                if novel.author:
                    identifiers[f"title_author:{novel.title}|{novel.author}"] = novel.id
                else:
                    identifiers[f"title:{novel.title}"] = novel.id

            # 更新Redis缓存（5分钟过期）
            if use_cache:
                try:
                    from src.utils.redis_client import get_redis_client
                    redis_client = get_redis_client()
                    if redis_client:
                        redis_client.set("novel_identifiers", identifiers, expire=300)
                        logger.info(f"已缓存小说标识符到Redis: {len(identifiers)} 条")
                except Exception as e:
                    logger.warning(f"缓存到Redis失败: {e}")

            return identifiers

    def invalidate_novel_identifiers_cache(self):
        """使小说标识符缓存失效（在新增/删除小说后调用）"""
        try:
            from src.utils.redis_client import get_redis_client
            redis_client = get_redis_client()
            if redis_client:
                redis_client.delete("novel_identifiers")
                logger.info("已清除Redis中的小说标识符缓存")
        except Exception as e:
            logger.warning(f"清除Redis缓存失败: {e}")

    def check_novel_exists(self, title: str, author: str = None, file_path: str = None) -> Optional[Novel]:
        """检查小说是否已存在

        Args:
            title: 小说标题
            author: 作者（可选）
            file_path: 文件路径（可选）

        Returns:
            如果存在返回小说对象，否则返回None
        """
        with self.get_session() as session:
            query = session.query(Novel)

            # 优先使用文件路径判断（更准确）
            if file_path:
                novel = query.filter(Novel.file_path == file_path).first()
                if novel:
                    return novel

            # 使用标题和作者判断
            if author:
                novel = query.filter(
                    Novel.title == title,
                    Novel.author == author
                ).first()
            else:
                novel = query.filter(Novel.title == title).first()

            return novel

    def get_all_novels(self) -> List[Novel]:
        with self.get_session() as session:
            return session.query(Novel).order_by(desc(Novel.created_at)).all()
    
    def get_novels_paginated(self, page: int = 1, per_page: int = 10,
                           search: str = None, tag_filter: str = None, type_filter: str = None,
                           sort_by: str = 'created_at', sort_order: str = 'desc') -> Dict[str, Any]:
        """高效的分页小说查询，支持搜索、标签、类型过滤和排序"""
        with self.get_session() as session:
            # 构建基础查询
            query = session.query(Novel)

            # 应用搜索过滤
            if search:
                search_term = f'%{search}%'
                query = query.filter(
                    Novel.title.ilike(search_term) |
                    Novel.author.ilike(search_term) |
                    Novel.description.ilike(search_term)
                )

            # 应用标签过滤（使用JSON查询）
            if tag_filter:
                # 对于MySQL/PostgreSQL，使用JSON_CONTAINS
                # 对于SQLite，使用简单的LIKE查询
                if 'mysql' in str(session.bind.url):
                    query = query.filter(func.json_contains(Novel.tags, f'"{tag_filter}"'))
                else:
                    # SQLite fallback
                    query = query.filter(Novel.tags.ilike(f'%{tag_filter}%'))

            # 应用类型过滤（基于字数）
            if type_filter:
                if type_filter == 'short':
                    # 短篇：2万字及以下
                    query = query.filter(Novel.total_word_count <= 20000)
                elif type_filter == 'long':
                    # 长篇：超过2万字
                    query = query.filter(Novel.total_word_count > 20000)

            # 应用排序
            sort_column = Novel.created_at  # 默认排序字段
            if sort_by == 'title':
                sort_column = Novel.title
            elif sort_by == 'author':
                sort_column = Novel.author
            elif sort_by == 'total_chapters':
                sort_column = Novel.total_chapters
            elif sort_by == 'file_size':
                sort_column = Novel.file_size
            elif sort_by == 'total_word_count':
                sort_column = Novel.total_word_count
            elif sort_by == 'created_at':
                sort_column = Novel.created_at
            elif sort_by == 'updated_at':
                sort_column = Novel.updated_at

            # 应用排序方向
            if sort_order == 'desc':
                query = query.order_by(desc(sort_column))
            else:
                query = query.order_by(asc(sort_column))

            # 获取总数（在应用分页之前）
            total_count = query.count()

            # 应用分页
            offset = (page - 1) * per_page
            novels = query.offset(offset).limit(per_page).all()

            # 构建结果
            return {
                'novels': novels,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': total_count,
                    'total_pages': (total_count + per_page - 1) // per_page if total_count > 0 else 0
                }
            }
    
    def get_novels_query(self):
        """获取小说查询对象，用于分页和筛选"""
        session = self.db_manager.get_session()
        return session.query(Novel).order_by(desc(Novel.created_at))
    
    def update_novel(self, novel_id: int, **kwargs) -> Optional[Novel]:
        with self.get_session() as session:
            novel = session.query(Novel).filter(Novel.id == novel_id).first()
            if novel:
                for key, value in kwargs.items():
                    if hasattr(novel, key):
                        setattr(novel, key, value)
                novel.updated_at = beijing_now()
                session.commit()
                session.refresh(novel)
                return novel
            return None
    
    def delete_novel(self, novel_id: int) -> bool:
        with self.get_session() as session:
            novel = session.query(Novel).filter(Novel.id == novel_id).first()
            if novel:
                logger.info(f"开始删除小说 - ID: {novel_id}, 标题: {novel.title}, 章节已解析: {novel.chapters_parsed}")

                # 如果小说没有解析章节列表，可以快速删除
                if not novel.chapters_parsed:
                    logger.info(f"  小说未解析章节，执行快速删除")
                    # 直接删除小说记录（由于外键级联，相关数据会自动删除）
                    session.delete(novel)
                    session.commit()
                    logger.info(f"✅ 小说快速删除完成 - ID: {novel_id}, 标题: {novel.title}")

                    # 清除小说标识符缓存
                    self.invalidate_novel_identifiers_cache()
                    return True

                # 如果已解析章节，执行完整的删除流程
                # Delete knowledge graph tasks
                kg_tasks_count = session.query(KnowledgeGraphTask).filter(
                    KnowledgeGraphTask.novel_id == novel_id
                ).count()
                if kg_tasks_count > 0:
                    logger.info(f"  删除知识图谱任务: {kg_tasks_count} 个")
                    session.query(KnowledgeGraphTask).filter(
                        KnowledgeGraphTask.novel_id == novel_id
                    ).delete()

                # Delete related analysis tasks
                analysis_tasks = session.query(AnalysisTask).filter(AnalysisTask.novel_id == novel_id).all()
                if analysis_tasks:
                    logger.info(f"  删除分析任务: {len(analysis_tasks)} 个")
                    for task in analysis_tasks:
                        # Delete task chapter statuses for analysis tasks
                        status_count = session.query(TaskChapterStatus).filter(
                            TaskChapterStatus.task_id == task.id,
                            TaskChapterStatus.task_type == 'analysis'
                        ).count()
                        if status_count > 0:
                            logger.info(f"    删除任务章节状态 (任务ID: {task.id}): {status_count} 条")
                            session.query(TaskChapterStatus).filter(
                                TaskChapterStatus.task_id == task.id,
                                TaskChapterStatus.task_type == 'analysis'
                            ).delete()
                        # Delete the analysis task
                        session.delete(task)

                # Delete workflow executions and related task chapter statuses
                workflow_executions = session.query(WorkflowExecution).filter(
                    WorkflowExecution.novel_id == novel_id
                ).all()
                if workflow_executions:
                    logger.info(f"  删除工作流执行记录: {len(workflow_executions)} 个")
                    for execution in workflow_executions:
                        # Delete task chapter statuses for workflow executions
                        status_count = session.query(TaskChapterStatus).filter(
                            TaskChapterStatus.task_id == execution.id,
                            TaskChapterStatus.task_type == 'workflow'
                        ).count()
                        if status_count > 0:
                            logger.info(f"    删除工作流章节状态 (执行ID: {execution.id}): {status_count} 条")
                            session.query(TaskChapterStatus).filter(
                                TaskChapterStatus.task_id == execution.id,
                                TaskChapterStatus.task_type == 'workflow'
                            ).delete()
                        # Delete the workflow execution
                        session.delete(execution)

                # Delete all chapters (must be done before deleting novel due to foreign key constraints)
                chapters = session.query(Chapter).filter(Chapter.novel_id == novel_id).all()
                if chapters:
                    chapter_count = len(chapters)
                    logger.info(f"  删除章节: {chapter_count} 个")

                    # Get all chapter IDs for batch operations
                    chapter_ids = [chapter.id for chapter in chapters]

                    # Batch delete analyses associated with chapters
                    logger.info(f"    开始批量删除章节关联的分析结果...")
                    analyses_count = session.query(Analysis).filter(Analysis.chapter_id.in_(chapter_ids)).count()
                    if analyses_count > 0:
                        logger.info(f"    找到 {analyses_count} 条分析结果")
                        session.query(Analysis).filter(Analysis.chapter_id.in_(chapter_ids)).delete(synchronize_session=False)
                        logger.info(f"    已删除 {analyses_count} 条章节关联的分析结果")
                    else:
                        logger.info(f"    没有找到章节关联的分析结果")

                    # Batch delete chapters
                    logger.info(f"    开始批量删除 {chapter_count} 个章节...")
                    session.query(Chapter).filter(Chapter.novel_id == novel_id).delete(synchronize_session=False)
                    logger.info(f"    已删除 {chapter_count} 个章节")

                # Delete analyses directly associated with the novel (if any)
                novel_analyses_count = session.query(Analysis).filter(Analysis.novel_id == novel_id).count()
                if novel_analyses_count > 0:
                    logger.info(f"  删除小说直接关联的分析结果: {novel_analyses_count} 条")
                    session.query(Analysis).filter(Analysis.novel_id == novel_id).delete()

                # Delete the novel
                logger.info(f"  删除小说记录")
                session.delete(novel)
                session.commit()
                logger.info(f"✅ 小说删除完成 - ID: {novel_id}, 标题: {novel.title}")

                # 清除小说标识符缓存
                self.invalidate_novel_identifiers_cache()

                return True
            else:
                logger.warning(f"小说不存在 - ID: {novel_id}")
                return False
    
    # Chapter operations
    def create_chapter(self, novel_id: int, title: str, content: str,
                      chapter_number: int) -> Chapter:
        """
        创建章节（性能优化版）
        注意：不再每次插入时更新小说统计，改为在所有章节导入完成后统一更新
        """
        with self.get_session() as session:
            chapter = Chapter(
                novel_id=novel_id,
                title=title,
                content=content,
                chapter_number=chapter_number,
                word_count=len(content)
            )
            session.add(chapter)
            session.commit()
            session.refresh(chapter)
            return chapter

    def bulk_create_chapters(self, chapters_data: List[dict]) -> int:
        """
        批量创建章节（高性能版本）

        Args:
            chapters_data: 章节数据列表，每个元素包含 {novel_id, title, content, chapter_number}

        Returns:
            插入的章节数量
        """
        if not chapters_data:
            return 0

        with self.get_session() as session:
            # 准备批量插入数据
            now = beijing_now()
            mappings = []
            for data in chapters_data:
                mappings.append({
                    'novel_id': data['novel_id'],
                    'title': data['title'],
                    'content': data['content'],
                    'chapter_number': data['chapter_number'],
                    'word_count': len(data['content']),
                    'created_at': now
                })

            # 使用 bulk_insert_mappings 批量插入
            session.bulk_insert_mappings(Chapter, mappings)
            session.commit()

            return len(mappings)

    def get_chapter_by_id(self, chapter_id: int) -> Optional[Chapter]:
        with self.get_session() as session:
            return session.query(Chapter).filter(Chapter.id == chapter_id).first()
    
    def get_chapters_by_novel(self, novel_id: int, page: int = 1, per_page: int = 20) -> tuple:
        with self.get_session() as session:
            offset = (page - 1) * per_page
            
            # 优化：使用索引查询和只选择必要字段
            # 获取总数（利用 idx_chapters_novel_id 索引）
            total_count = session.query(func.count(Chapter.id)).filter(Chapter.novel_id == novel_id).scalar()
            
            # 优化：获取分页数据，只查询需要的字段，利用复合索引 idx_chapters_novel_chapter_number
            chapters = session.query(Chapter.id, Chapter.title, Chapter.chapter_number, 
                                   Chapter.word_count, Chapter.created_at, Chapter.novel_id)\
                         .filter(Chapter.novel_id == novel_id)\
                         .order_by(Chapter.chapter_number)\
                         .offset(offset).limit(per_page).all()
            
            # 转换为对象形式以保持兼容性
            chapter_objects = []
            for ch in chapters:
                # 创建一个类似Chapter对象的简化版本
                chapter_obj = type('Chapter', (), {
                    'id': ch.id,
                    'title': ch.title, 
                    'chapter_number': ch.chapter_number,
                    'word_count': ch.word_count,
                    'created_at': ch.created_at,
                    'novel_id': ch.novel_id
                })()
                chapter_objects.append(chapter_obj)
            
            # 计算分页信息
            total_pages = (total_count + per_page - 1) // per_page
            has_prev = page > 1
            has_next = page < total_pages
            
            pagination = {
                'page': page,
                'per_page': per_page,
                'total_count': total_count,
                'total_pages': total_pages,
                'has_prev': has_prev,
                'has_next': has_next
            }
            
            return chapter_objects, pagination
    
    def get_chapters_with_task_counts(self, novel_id: int, page: int = 1, per_page: int = 20) -> tuple:
        """获取章节列表并批量查询任务数量，使用原生SQL优化性能"""
        with self.get_session() as session:
            from sqlalchemy import text
            offset = (page - 1) * per_page
            
            # 获取总数（利用 idx_chapters_novel_id 索引）
            total_count = session.execute(
                text('SELECT COUNT(*) FROM chapters WHERE novel_id = :novel_id'),
                {'novel_id': novel_id}
            ).scalar()
            
            # 章节列表不需要content字段，显著提升性能
            chapters_sql = text("""
                SELECT 
                    c.id, c.novel_id, c.title, '' as content, c.chapter_number, 
                    c.word_count, c.created_at,
                    COALESCE(tc.task_count, 0) as task_count
                FROM chapters c
                LEFT JOIN (
                    SELECT chapter_id, COUNT(*) as task_count 
                    FROM task_chapter_status 
                    GROUP BY chapter_id
                ) tc ON c.id = tc.chapter_id
                WHERE c.novel_id = :novel_id
                ORDER BY c.chapter_number
                LIMIT :per_page OFFSET :offset
            """)
            
            result = session.execute(chapters_sql, {
                'novel_id': novel_id,
                'per_page': per_page,
                'offset': offset
            }).fetchall()
            
            # 将结果转换为类似ORM对象的格式
            from collections import namedtuple
            ChapterResult = namedtuple('Chapter', [
                'id', 'novel_id', 'title', 'content', 'chapter_number', 
                'word_count', 'created_at', 'task_count'
            ])
            
            chapters = []
            for row in result:
                chapter = ChapterResult(*row)
                chapters.append(chapter)
            
            # 计算分页信息
            total_pages = (total_count + per_page - 1) // per_page
            has_prev = page > 1
            has_next = page < total_pages
            
            pagination = {
                'page': page,
                'per_page': per_page,
                'total_count': total_count,
                'total_pages': total_pages,
                'has_prev': has_prev,
                'has_next': has_next
            }
            
            return chapters, pagination
    
    def get_all_chapters_by_novel(self, novel_id: int) -> List[Chapter]:
        with self.get_session() as session:
            return session.query(Chapter).filter(Chapter.novel_id == novel_id)\
                         .order_by(Chapter.chapter_number).all()
    
    def get_chapters_for_sampling(self, novel_id: int) -> List[Dict]:
        """获取用于智能采样的章节数据（只包含摘要，不包含完整内容）"""
        with self.get_session() as session:
            from sqlalchemy import text
            
            # 使用原生SQL只获取前500个字符的内容，大幅提升性能
            result = session.execute(text("""
                SELECT 
                    id, title, chapter_number, word_count,
                    LEFT(content, 500) as content_preview
                FROM chapters 
                WHERE novel_id = :novel_id 
                ORDER BY chapter_number
            """), {'novel_id': novel_id}).fetchall()
            
            # 转换为字典格式
            chapters = []
            for row in result:
                chapters.append({
                    'id': row[0],
                    'title': row[1],
                    'content': row[4],  # 使用content_preview作为content
                    'chapter_number': row[2],
                    'word_count': row[3]
                })
            
            return chapters
    
    def get_chapters_count_only(self, novel_id: int) -> int:
        """只获取章节数量，用于快速生成建议"""
        with self.get_session() as session:
            from sqlalchemy import text
            result = session.execute(text("""
                SELECT COUNT(*) FROM chapters WHERE novel_id = :novel_id
            """), {'novel_id': novel_id}).scalar()
            return result or 0
    
    def get_chapters_by_novel_id(self, novel_id: int) -> List[Chapter]:
        """获取小说的所有章节（别名方法，为了兼容性）"""
        return self.get_all_chapters_by_novel(novel_id)
    
    def update_chapter(self, chapter_id: int, **kwargs) -> Optional[Chapter]:
        with self.get_session() as session:
            chapter = session.query(Chapter).filter(Chapter.id == chapter_id).first()
            if chapter:
                for key, value in kwargs.items():
                    if hasattr(chapter, key):
                        setattr(chapter, key, value)
                if 'content' in kwargs:
                    chapter.word_count = len(kwargs['content'])
                session.commit()
                session.refresh(chapter)
                return chapter
            return None
    
    def delete_chapter(self, chapter_id: int) -> bool:
        with self.get_session() as session:
            chapter = session.query(Chapter).filter(Chapter.id == chapter_id).first()
            if chapter:
                novel_id = chapter.novel_id
                session.delete(chapter)
                
                # 更新小说的章节总数
                novel = session.query(Novel).filter(Novel.id == novel_id).first()
                if novel:
                    novel.total_chapters = session.query(func.count(Chapter.id)).filter(Chapter.novel_id == novel_id).scalar() - 1
                    novel.updated_at = beijing_now()
                
                session.commit()
                return True
            return False
    
    # PromptTemplate operations
    def create_prompt_template(self, name: str, category: str, description: str,
                              template: str, variables: List[str] = None,
                              variable_values: dict = None, is_default: bool = False) -> PromptTemplate:
        with self.get_session() as session:
            prompt_template = PromptTemplate(
                name=name,
                category=category,
                description=description,
                template=template,
                variables=variables or [],
                variable_values=variable_values or {},
                is_default=is_default
            )
            session.add(prompt_template)
            session.commit()
            session.refresh(prompt_template)
            return prompt_template
    
    def get_prompt_template_by_id(self, template_id: int) -> Optional[PromptTemplate]:
        with self.get_session() as session:
            return session.query(PromptTemplate).filter(PromptTemplate.id == template_id).first()
    
    def get_all_prompt_templates(self) -> List[PromptTemplate]:
        with self.get_session() as session:
            return session.query(PromptTemplate).order_by(PromptTemplate.category, PromptTemplate.name).all()
    
    def get_prompt_templates_by_category(self, category: str) -> List[PromptTemplate]:
        with self.get_session() as session:
            return session.query(PromptTemplate).filter(PromptTemplate.category == category)\
                         .order_by(PromptTemplate.name).all()
    
    def get_prompt_template_categories(self) -> List[str]:
        with self.get_session() as session:
            return [row[0] for row in session.query(PromptTemplate.category).distinct().all()]
    
    def update_prompt_template(self, template_id: int, **kwargs) -> Optional[PromptTemplate]:
        with self.get_session() as session:
            template = session.query(PromptTemplate).filter(PromptTemplate.id == template_id).first()
            if template:
                for key, value in kwargs.items():
                    if hasattr(template, key):
                        setattr(template, key, value)
                template.updated_at = beijing_now()
                session.commit()
                session.refresh(template)
                return template
            return None
    
    def delete_prompt_template(self, template_id: int) -> bool:
        with self.get_session() as session:
            template = session.query(PromptTemplate).filter(PromptTemplate.id == template_id).first()
            if template:
                session.delete(template)
                session.commit()
                return True
            return False
    
    # AIProvider operations
    def create_ai_provider(self, name: str, display_name: str, api_key: str = None,
                          base_url: str = None, models: List[str] = None,
                          is_active: bool = True) -> AIProvider:
        with self.get_session() as session:
            provider = AIProvider(
                name=name,
                display_name=display_name,
                api_key=api_key,
                base_url=base_url,
                models=models or [],
                is_active=is_active
            )
            session.add(provider)
            session.commit()
            session.refresh(provider)
            return provider
    
    def get_ai_provider_by_id(self, provider_id: int) -> Optional[AIProvider]:
        with self.get_session() as session:
            return session.query(AIProvider).filter(AIProvider.id == provider_id).first()
    
    def get_ai_provider_by_name(self, name: str) -> Optional[AIProvider]:
        with self.get_session() as session:
            return session.query(AIProvider).filter(AIProvider.name == name).first()
    
    def get_all_ai_providers(self) -> List[AIProvider]:
        with self.get_session() as session:
            return session.query(AIProvider).order_by(AIProvider.name).all()
    
    def update_ai_provider(self, provider_id: int, **kwargs) -> Optional[AIProvider]:
        logger.info(f"更新AI服务商 - ID: {provider_id}, 参数: {kwargs}")
        with self.get_session() as session:
            provider = session.query(AIProvider).filter(AIProvider.id == provider_id).first()
            if provider:
                logger.info(f"找到服务商 - 名称: {provider.name}, 当前API密钥: {provider.api_key[:10] + '...' if provider.api_key else 'None'}")
                for key, value in kwargs.items():
                    if hasattr(provider, key):
                        old_value = getattr(provider, key)
                        setattr(provider, key, value)
                        logger.info(f"更新字段 {key}: {old_value} -> {value}")
                    else:
                        logger.warning(f"字段 {key} 不存在于AIProvider模型中")
                provider.updated_at = beijing_now()
                session.commit()
                session.refresh(provider)
                logger.info(f"服务商更新完成 - 新API密钥: {provider.api_key[:10] + '...' if provider.api_key else 'None'}")
                return provider
            else:
                logger.error(f"未找到ID为 {provider_id} 的AI服务商")
            return None
    
    def delete_ai_provider(self, provider_id: int) -> bool:
        with self.get_session() as session:
            provider = session.query(AIProvider).filter(AIProvider.id == provider_id).first()
            if not provider:
                return False
                
            # 检查是否有引用此AI服务商的分析记录
            analysis_count = session.query(Analysis).filter(Analysis.ai_provider_id == provider_id).count()
            if analysis_count > 0:
                raise ValueError(f"无法删除AI服务商，仍有 {analysis_count} 条分析记录正在使用此服务商。请先删除相关分析记录。")
            
            # 检查是否有引用此AI服务商的分析任务
            task_count = session.query(AnalysisTask).filter(AnalysisTask.ai_provider_id == provider_id).count()
            if task_count > 0:
                raise ValueError(f"无法删除AI服务商，仍有 {task_count} 个分析任务正在使用此服务商。请先删除相关分析任务。")
            
            # 检查是否有引用此AI服务商的工作流步骤
            step_count = session.query(WorkflowStep).filter(WorkflowStep.ai_provider_id == provider_id).count()
            if step_count > 0:
                raise ValueError(f"无法删除AI服务商，仍有 {step_count} 个工作流步骤正在使用此服务商。请先修改相关工作流配置。")
            
            # 检查是否有引用此AI服务商的工作流步骤执行记录
            step_exec_count = session.query(WorkflowStepExecution).filter(WorkflowStepExecution.ai_provider_id == provider_id).count()
            if step_exec_count > 0:
                raise ValueError(f"无法删除AI服务商，仍有 {step_exec_count} 条工作流执行记录使用此服务商。请先清理相关执行历史。")
            
            # 如果没有任何引用，才删除AI服务商
            session.delete(provider)
            session.commit()
            return True
    
    # Workflow operations
    def get_workflow_by_id(self, workflow_id: int) -> Optional[Workflow]:
        with self.get_session() as session:
            return session.query(Workflow).filter(Workflow.id == workflow_id).first()
    
    # Analysis operations
    def create_analysis(self, novel_id: int = None, chapter_id: int = None, 
                       prompt_template_id: int = None, ai_provider_id: int = None, 
                       model_name: str = None, analysis_type: str = None,
                       input_text: str = None, output_text: str = None,
                       status: str = 'pending', error_message: str = None,
                       input_tokens: int = 0, output_tokens: int = 0,
                       total_tokens: int = 0, estimated_cost: float = 0.0,
                       task_id: int = None) -> Analysis:
        with self.get_session() as session:
            analysis = Analysis(
                novel_id=novel_id,
                chapter_id=chapter_id,
                prompt_template_id=prompt_template_id,
                ai_provider_id=ai_provider_id,
                model_name=model_name,
                analysis_type=analysis_type,
                input_text=input_text or "",
                output_text=output_text,
                status=status,
                error_message=error_message,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
                estimated_cost=estimated_cost,
                task_id=task_id
            )
            session.add(analysis)
            session.commit()
            session.refresh(analysis)
            return analysis
    
    def update_analysis(self, analysis_id: int, **kwargs) -> Optional[Analysis]:
        with self.get_session() as session:
            analysis = session.query(Analysis).filter(Analysis.id == analysis_id).first()
            if analysis:
                for key, value in kwargs.items():
                    if hasattr(analysis, key):
                        setattr(analysis, key, value)
                if kwargs.get('status') == 'completed':
                    analysis.completed_at = beijing_now()
                session.commit()
                session.refresh(analysis)
                return analysis
            return None
    
    def get_analyses_by_chapter(self, chapter_id: int) -> List[Analysis]:
        with self.get_session() as session:
            return session.query(Analysis).filter(Analysis.chapter_id == chapter_id)\
                         .order_by(desc(Analysis.created_at)).all()
    
    def get_analyses_by_novel(self, novel_id: int) -> List[Analysis]:
        with self.get_session() as session:
            return session.query(Analysis).filter(Analysis.novel_id == novel_id)\
                         .order_by(desc(Analysis.created_at)).all()
    
    def get_analysis_by_id(self, analysis_id: int) -> Optional[Analysis]:
        with self.get_session() as session:
            return session.query(Analysis).filter(Analysis.id == analysis_id).first()
    
    # AnalysisTask operations
    def create_analysis_task(self, task_name: str, description: str, novel_id: int,
                           chapter_ids: List[int], prompt_template_id: int,
                           ai_provider_id: int, model_name: str, workflow_id: int = None) -> AnalysisTask:
        with self.get_session() as session:
            task = AnalysisTask(
                task_name=task_name,
                description=description,
                novel_id=novel_id,
                chapter_ids=chapter_ids,
                prompt_template_id=prompt_template_id,
                ai_provider_id=ai_provider_id,
                model_name=model_name,
                workflow_id=workflow_id,
                total_chapters=len(chapter_ids),
                status='created'
            )
            session.add(task)
            session.commit()
            session.refresh(task)
            
            
            return task
    
    def update_analysis_task(self, task_id: int, **kwargs) -> Optional[AnalysisTask]:
        with self.get_session() as session:
            task = session.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
            if task:
                for key, value in kwargs.items():
                    if hasattr(task, key):
                        setattr(task, key, value)
                if kwargs.get('status') == 'completed':
                    task.completed_at = beijing_now()
                elif kwargs.get('status') == 'running' and not task.started_at:
                    task.started_at = beijing_now()
                session.commit()
                session.refresh(task)
                return task
            return None
    
    def get_analysis_tasks(self) -> List[AnalysisTask]:
        with self.get_session() as session:
            return session.query(AnalysisTask).order_by(desc(AnalysisTask.created_at)).all()
    
    def get_analysis_task_by_id(self, task_id: int) -> Optional[AnalysisTask]:
        with self.get_session() as session:
            return session.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
    
    def delete_analysis_task(self, task_id: int) -> bool:
        """删除分析任务及其相关数据"""
        try:
            with self.get_session() as session:
                task = session.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
                if task:
                    # 删除相关的workflow_executions记录
                    workflow_executions = session.query(WorkflowExecution).filter(
                        WorkflowExecution.task_id == task_id
                    ).all()
                    for execution in workflow_executions:
                        session.delete(execution)
                    
                    # 删除相关的analyses记录
                    try:
                        analyses = session.query(Analysis).filter(Analysis.task_id == task_id).all()
                        for analysis in analyses:
                            session.delete(analysis)
                    except Exception as analyses_error:
                        # 如果task_id列不存在，记录警告但继续执行
                        logger.warning(f"删除analyses记录失败（可能是列不存在）: {analyses_error}")
                        pass
                    
                    # 删除相关的task_chapter_status记录
                    task_chapter_statuses = session.query(TaskChapterStatus).filter(
                        TaskChapterStatus.task_id == task_id
                    ).all()
                    for status in task_chapter_statuses:
                        session.delete(status)
                    
                    # 最后删除任务本身
                    session.delete(task)
                    session.commit()
                    logger.info(f"成功删除分析任务 {task_id} 及其相关数据")
                    return True
                return False
        except Exception as e:
            logger.error(f"删除分析任务失败: {str(e)}")
            return False
    
    def get_system_statistics(self) -> Dict[str, Any]:
        """获取系统统计信息（优化版本）"""
        with self.get_session() as session:
            try:
                # 使用单个查询获取基础统计
                novel_count = session.query(func.count(Novel.id)).scalar() or 0
                chapter_count = session.query(func.count(Chapter.id)).scalar() or 0

                # 使用更高效的任务状态统计查询
                task_stats = session.query(
                    func.count(AnalysisTask.id).label('total'),
                    func.sum(case((AnalysisTask.status == 'completed', 1), else_=0)).label('completed'),
                    func.sum(case((AnalysisTask.status == 'running', 1), else_=0)).label('running'),
                    func.sum(case((AnalysisTask.status.in_(['created', 'running']), 1), else_=0)).label('active')
                ).first()

                # 获取情节总数（从Neo4j）
                plot_count = 0
                try:
                    from src.services.knowledge_graph_service import get_kg_service
                    kg_service = get_kg_service()
                    # 查询Neo4j中的Plot节点总数
                    with kg_service.driver.session() as neo4j_session:
                        result = neo4j_session.run("MATCH (p:Plot) RETURN count(p) as count")
                        record = result.single()
                        if record:
                            plot_count = record['count']
                except Exception as neo4j_error:
                    # Neo4j查询失败不影响整体统计，使用默认值0
                    logger.warning(f"获取情节总数失败: {neo4j_error}")
                    plot_count = 0

                # 快速返回统计信息，避免复杂的token和cost计算
                stats = {
                    'total_novels': novel_count,
                    'total_chapters': chapter_count,
                    'total_tasks': task_stats.total or 0,
                    'total_plots': plot_count,
                    'completed_tasks': task_stats.completed or 0,
                    'running_tasks': task_stats.running or 0,
                    'active_tasks': task_stats.active or 0,
                    'total_tokens': 0,  # 暂时跳过这个慢查询
                    'estimated_total_cost': 0.0  # 暂时跳过这个慢查询
                }
                return stats
            except Exception as e:
                # 如果查询失败，返回默认值
                return {
                    'total_novels': 0,
                    'total_chapters': 0,
                    'total_tasks': 0,
                    'total_plots': 0,
                    'completed_tasks': 0,
                    'running_tasks': 0,
                    'active_tasks': 0,
                    'total_tokens': 0,
                    'estimated_total_cost': 0.0
                }
    
    def get_running_workflow_executions_for_chapter(self, chapter_id: int) -> List[WorkflowExecution]:
        """获取指定章节的运行中工作流执行任务"""
        with self.get_session() as session:
            return session.query(WorkflowExecution).filter(
                WorkflowExecution.chapter_id == chapter_id,
                WorkflowExecution.status.in_(['created', 'running'])
            ).order_by(desc(WorkflowExecution.created_at)).all()
    
    def get_workflow_execution_current_step(self, execution_id: int) -> Optional[Dict[str, Any]]:
        """获取工作流执行的当前步骤信息"""
        with self.get_session() as session:
            execution = session.query(WorkflowExecution).filter(
                WorkflowExecution.id == execution_id
            ).first()
            
            if not execution:
                return None
            
            # 获取工作流的所有步骤
            workflow_steps = session.query(WorkflowStep).filter(
                WorkflowStep.workflow_id == execution.workflow_id
            ).order_by(WorkflowStep.order).all()
            
            if not workflow_steps or execution.current_step >= len(workflow_steps):
                return None
            
            current_step = workflow_steps[execution.current_step]
            
            # 获取当前步骤的执行状态
            step_execution = session.query(WorkflowStepExecution).filter(
                WorkflowStepExecution.execution_id == execution_id,
                WorkflowStepExecution.step_id == current_step.id
            ).first()
            
            return {
                'step_name': current_step.name,
                'step_order': execution.current_step + 1,
                'total_steps': len(workflow_steps),
                'step_status': step_execution.status if step_execution else 'pending',
                'workflow_name': execution.workflow.name
            }
    
    # TaskChapterStatus 相关方法
    def create_task_chapter_status(self, task_id: int, task_type: str, chapter_id: int, 
                                 status: str = 'pending', current_step: str = None, 
                                 step_order: int = None, total_steps: int = None) -> TaskChapterStatus:
        """创建任务章节状态记录（如果不存在）"""
        with self.get_session() as session:
            # 先检查是否已存在记录
            existing_status = session.query(TaskChapterStatus).filter(
                TaskChapterStatus.task_id == task_id,
                TaskChapterStatus.task_type == task_type,
                TaskChapterStatus.chapter_id == chapter_id
            ).first()
            
            if existing_status:
                # 如果已存在，返回现有记录，不重复创建
                return existing_status
            
            # 不存在则创建新记录
            task_status = TaskChapterStatus(
                task_id=task_id,
                task_type=task_type,
                chapter_id=chapter_id,
                status=status,
                current_step=current_step,
                step_order=step_order,
                total_steps=total_steps
            )
            session.add(task_status)
            session.commit()
            session.refresh(task_status)
            return task_status
    
    def update_task_chapter_status(self, task_id: int, task_type: str, chapter_id: int, 
                                 status: str = None, current_step: str = None, 
                                 step_order: int = None, total_steps: int = None):
        """更新任务章节状态"""
        with self.get_session() as session:
            task_status = session.query(TaskChapterStatus).filter(
                TaskChapterStatus.task_id == task_id,
                TaskChapterStatus.task_type == task_type,
                TaskChapterStatus.chapter_id == chapter_id
            ).first()
            
            if task_status:
                if status is not None:
                    task_status.status = status
                if current_step is not None:
                    task_status.current_step = current_step
                if step_order is not None:
                    task_status.step_order = step_order
                if total_steps is not None:
                    task_status.total_steps = total_steps
                
                task_status.updated_at = beijing_now()
                session.commit()
                return task_status
            return None
    
    def get_task_chapter_statuses(self, task_id: int = None, task_type: str = None, 
                                chapter_id: int = None, status: str = None):
        """获取任务章节状态列表"""
        with self.get_session() as session:
            query = session.query(TaskChapterStatus)
            
            if task_id is not None:
                query = query.filter(TaskChapterStatus.task_id == task_id)
            if task_type is not None:
                query = query.filter(TaskChapterStatus.task_type == task_type)
            if chapter_id is not None:
                query = query.filter(TaskChapterStatus.chapter_id == chapter_id)
            if status is not None:
                query = query.filter(TaskChapterStatus.status == status)
            
            return query.order_by(TaskChapterStatus.updated_at.desc()).all()
    
    def delete_task_chapter_statuses(self, task_id: int, task_type: str):
        """删除指定任务的所有章节状态记录"""
        with self.get_session() as session:
            session.query(TaskChapterStatus).filter(
                TaskChapterStatus.task_id == task_id,
                TaskChapterStatus.task_type == task_type
            ).delete()
            session.commit()
    
    def get_running_chapter_statuses(self):
        """获取所有章节状态（包括completed状态，用于章节浏览显示）"""
        with self.get_session() as session:
            return session.query(TaskChapterStatus).filter(
                TaskChapterStatus.status.in_(['pending', 'running', 'completed', 'failed'])
            ).order_by(TaskChapterStatus.updated_at.desc()).all()
    
    def delete_task_chapter_statuses_by_task_id(self, task_id: int, task_type: str):
        """根据任务ID和任务类型删除TaskChapterStatus记录"""
        try:
            with self.get_session() as session:
                deleted_count = session.query(TaskChapterStatus).filter(
                    TaskChapterStatus.task_id == task_id,
                    TaskChapterStatus.task_type == task_type
                ).delete()
                session.commit()
                logger.info(f"删除任务 {task_id} (类型: {task_type}) 的 {deleted_count} 条TaskChapterStatus记录")
                return deleted_count
        except Exception as e:
            logger.error(f"删除TaskChapterStatus记录失败: task_id={task_id}, task_type={task_type}, 错误: {e}")
            raise
    
    def delete_related_analysis_data(self, task_id: int, novel_id: int, chapter_ids: list, 
                                   prompt_template_id: int, ai_provider_id: int, model_name: str):
        """删除与分析任务相关的Analysis记录"""
        try:
            with self.get_session() as session:
                # 删除相关的Analysis记录
                # 通过任务的配置参数来匹配相关的分析记录
                deleted_count = session.query(Analysis).filter(
                    Analysis.novel_id == novel_id,
                    Analysis.chapter_id.in_(chapter_ids),
                    Analysis.prompt_template_id == prompt_template_id,
                    Analysis.ai_provider_id == ai_provider_id,
                    Analysis.model_name == model_name
                ).delete(synchronize_session=False)
                
                session.commit()
                logger.info(f"删除任务 {task_id} 相关的 {deleted_count} 条Analysis记录")
                return deleted_count
        except Exception as e:
            logger.error(f"删除任务 {task_id} 相关Analysis记录失败: {e}")
            raise
    
    def delete_workflow_executions_by_novel_chapters(self, novel_id: int, chapter_ids: list = None):
        """删除指定小说和章节的工作流执行记录"""
        try:
            with self.get_session() as session:
                query = session.query(WorkflowExecution).filter(
                    WorkflowExecution.novel_id == novel_id
                )
                
                if chapter_ids:
                    query = query.filter(WorkflowExecution.chapter_id.in_(chapter_ids))
                
                # 获取要删除的执行记录ID
                execution_ids = [exec.id for exec in query.all()]
                
                if execution_ids:
                    # 删除工作流步骤执行记录（由于有cascade关系，会自动删除）
                    deleted_count = query.delete(synchronize_session=False)
                    session.commit()
                    
                    logger.info(f"删除 {deleted_count} 条WorkflowExecution记录及其相关的步骤执行记录")
                    return deleted_count
                else:
                    logger.info("没有找到需要删除的WorkflowExecution记录")
                    return 0
        except Exception as e:
            logger.error(f"删除WorkflowExecution记录失败: novel_id={novel_id}, chapter_ids={chapter_ids}, 错误: {e}")
            raise

    def delete_workflow_executions_by_task_ids(self, task_ids: List[int]) -> int:
        """根据任务ID删除工作流执行记录
        
        直接删除对应的WorkflowExecution和WorkflowStepExecution记录
        """
        try:
            with self.get_session() as session:
                # 查找WorkflowExecution记录
                workflow_executions = session.query(WorkflowExecution).filter(
                    WorkflowExecution.task_id.in_(task_ids)
                ).all()
                
                if not workflow_executions:
                    logger.info(f"没有找到task_ids {task_ids} 对应的WorkflowExecution记录")
                    return 0
                
                execution_ids = [execution.id for execution in workflow_executions]
                
                # 先删除WorkflowStepExecution记录（子表）
                step_execution_count = session.query(WorkflowStepExecution).filter(
                    WorkflowStepExecution.execution_id.in_(execution_ids)
                ).delete(synchronize_session=False)
                
                # 再删除WorkflowExecution记录（父表）
                execution_count = session.query(WorkflowExecution).filter(
                    WorkflowExecution.id.in_(execution_ids)
                ).delete(synchronize_session=False)
                
                session.commit()
                
                logger.info(f"删除了 {step_execution_count} 条WorkflowStepExecution记录和 {execution_count} 条WorkflowExecution记录")
                return execution_count
                
        except Exception as e:
            logger.error(f"删除工作流执行记录失败: {str(e)}")
            return 0
    
    def get_chapter_task_count(self, chapter_id: int) -> int:
        """获取章节的任务数量（优化版）"""
        try:
            with self.get_session() as session:
                # 优化：使用索引 idx_task_chapter_status_chapter 快速查询
                count = session.query(func.count(TaskChapterStatus.id)).filter(
                    TaskChapterStatus.chapter_id == chapter_id
                ).scalar()
                return count or 0
        except Exception as e:
            logger.error(f"获取章节 {chapter_id} 任务数量失败: {e}")
            return 0
    
    def get_chapters_task_counts_batch(self, chapter_ids: List[int]) -> Dict[int, int]:
        """批量获取多个章节的任务数量（优化版）"""
        try:
            with self.get_session() as session:
                # 批量查询所有章节的任务数量，减少数据库查询次数
                results = session.query(
                    TaskChapterStatus.chapter_id,
                    func.count(TaskChapterStatus.id).label('task_count')
                ).filter(
                    TaskChapterStatus.chapter_id.in_(chapter_ids)
                ).group_by(TaskChapterStatus.chapter_id).all()
                
                # 构建字典结果
                task_counts = {chapter_id: 0 for chapter_id in chapter_ids}
                for chapter_id, count in results:
                    task_counts[chapter_id] = count
                    
                return task_counts
        except Exception as e:
            logger.error(f"批量获取章节任务数量失败: {e}")
            return {chapter_id: 0 for chapter_id in chapter_ids}
    
    def get_analysis_task_with_details(self, task_id: int):
        """获取分析任务的详细信息，包括相关的分析记录数量"""
        try:
            with self.get_session() as session:
                task = session.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
                if not task:
                    return None
                
                # 统计相关的Analysis记录数量
                analysis_count = session.query(Analysis).filter(
                    Analysis.novel_id == task.novel_id,
                    Analysis.chapter_id.in_(task.chapter_ids),
                    Analysis.prompt_template_id == task.prompt_template_id,
                    Analysis.ai_provider_id == task.ai_provider_id,
                    Analysis.model_name == task.model_name
                ).count()
                
                # 统计相关的WorkflowExecution记录数量
                workflow_count = session.query(WorkflowExecution).filter(
                    WorkflowExecution.novel_id == task.novel_id,
                    WorkflowExecution.chapter_id.in_(task.chapter_ids)
                ).count()
                
                return {
                    'task': task,
                    'related_analysis_count': analysis_count,
                    'related_workflow_count': workflow_count
                }
        except Exception as e:
            logger.error(f"获取任务详细信息失败: task_id={task_id}, 错误: {e}")
            raise
    

# 全局数据库服务实例
db_service = DatabaseService()