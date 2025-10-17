import asyncio
import json
import traceback
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from jinja2 import Template

from src.ai.ai_service import get_ai_manager, AIServiceException
from src.services.database_service import db_service
from src.models.database import Analysis, AnalysisTask

logger = logging.getLogger(__name__)

class AnalysisService:
    """AI分析服务"""
    
    def __init__(self):
        self.db_service = db_service
    
    @property
    def ai_manager(self):
        """延迟获取AI管理器"""
        return get_ai_manager()
    
    def render_prompt(self, template_content: str, variables: Dict[str, Any]) -> str:
        """渲染提示词模板"""
        try:
            template = Template(template_content)
            return template.render(**variables)
        except Exception as e:
            logger.error(f"提示词模板渲染失败: {str(e)}\n堆栈信息:\n{traceback.format_exc()}")
            raise ValueError(f"提示词模板渲染失败: {str(e)}")
    
    async def analyze_chapter(self, chapter_id: int, prompt_template_id: int, 
                            ai_provider_id: int, model_name: str, 
                            additional_vars: Dict[str, Any] = None) -> Dict[str, Any]:
        """分析单个章节"""
        try:
            # 获取章节信息
            chapter = self.db_service.get_chapter_by_id(chapter_id)
            if not chapter:
                raise ValueError(f"章节不存在: {chapter_id}")
            
            # 获取提示词模板
            template = self.db_service.get_prompt_template_by_id(prompt_template_id)
            if not template:
                raise ValueError(f"提示词模板不存在: {prompt_template_id}")
            
            # 获取AI服务商
            provider = self.db_service.get_ai_provider_by_id(ai_provider_id)
            if not provider:
                raise ValueError(f"AI服务商不存在: {ai_provider_id}")
            
            # 准备模板变量
            variables = {
                'title': chapter.title,
                'content': chapter.content,
                'chapter_number': chapter.chapter_number,
                'word_count': chapter.word_count
            }
            
            # 添加额外变量
            if additional_vars:
                variables.update(additional_vars)
            
            # 渲染提示词
            prompt = self.render_prompt(template.template, variables)
            
            # 计算输入Token数量
            input_tokens = self.ai_manager.calculate_tokens(provider.name, prompt)
            
            # 创建分析记录
            analysis = self.db_service.create_analysis(
                novel_id=chapter.novel_id,
                chapter_id=chapter_id,
                prompt_template_id=prompt_template_id,
                ai_provider_id=ai_provider_id,
                model_name=model_name,
                analysis_type=template.category,
                input_text=prompt
            )
            
            # 更新分析状态为运行中
            self.db_service.update_analysis(analysis.id, 
                                          status='running',
                                          input_tokens=input_tokens)
            
            try:
                # 调用AI服务进行分析
                result = await self.ai_manager.generate_analysis(
                    provider.name, model_name, prompt
                )
                
                if result['success']:
                    # 计算成本
                    cost = self.ai_manager.estimate_cost(
                        provider.name, 
                        result['input_tokens'], 
                        result['output_tokens'], 
                        model_name
                    )
                    
                    # 更新分析结果
                    self.db_service.update_analysis(analysis.id,
                                                  status='completed',
                                                  output_text=result['content'],
                                                  input_tokens=result['input_tokens'],
                                                  output_tokens=result['output_tokens'],
                                                  total_tokens=result['total_tokens'],
                                                  estimated_cost=cost)
                    
                    return {
                        'success': True,
                        'analysis_id': analysis.id,
                        'content': result['content'],
                        'tokens': result['total_tokens'],
                        'cost': cost
                    }
                else:
                    # 分析失败
                    self.db_service.update_analysis(analysis.id,
                                                  status='failed',
                                                  error_message='AI分析失败')
                    return {
                        'success': False,
                        'error': 'AI分析失败',
                        'analysis_id': analysis.id
                    }
            
            except AIServiceException as e:
                # AI服务异常
                self.db_service.update_analysis(analysis.id,
                                              status='failed',
                                              error_message=str(e))
                return {
                    'success': False,
                    'error': str(e),
                    'analysis_id': analysis.id
                }
            
            except Exception as e:
                # 其他异常
                logger.error(f"分析章节时发生错误: {str(e)}\n堆栈信息:\n{traceback.format_exc()}")
                self.db_service.update_analysis(analysis.id,
                                              status='failed',
                                              error_message=str(e))
                return {
                    'success': False,
                    'error': str(e),
                    'analysis_id': analysis.id
                }
        
        except Exception as e:
            logger.error(f"章节分析失败: {str(e)}\n堆栈信息:\n{traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def analyze_chapter_sync(self, chapter_id: int, prompt_template_id: int, 
                           ai_provider_id: int, model_name: str, 
                           additional_vars: Dict[str, Any] = None) -> Dict[str, Any]:
        """分析单个章节（同步版本）"""
        try:
            # 获取章节信息
            chapter = self.db_service.get_chapter_by_id(chapter_id)
            if not chapter:
                raise ValueError(f"章节不存在: {chapter_id}")
            
            logger.info(f"  → 步骤1: 获取章节信息完成 - {chapter.title}")
            
            # 获取提示词模板
            template = self.db_service.get_prompt_template_by_id(prompt_template_id)
            if not template:
                raise ValueError(f"提示词模板不存在: {prompt_template_id}")
            
            logger.info(f"  → 步骤2: 获取提示词模板完成 - {template.name}")
            
            # 获取AI服务商
            provider = self.db_service.get_ai_provider_by_id(ai_provider_id)
            if not provider:
                raise ValueError(f"AI服务商不存在: {ai_provider_id}")
            
            logger.info(f"  → 步骤3: 获取AI服务商完成 - {provider.name}")
            
            # 准备模板变量
            variables = {
                'title': chapter.title,
                'content': chapter.content,
                'chapter_number': chapter.chapter_number,
                'word_count': chapter.word_count
            }
            
            # 添加额外变量
            if additional_vars:
                variables.update(additional_vars)
            
            # 渲染提示词
            prompt = self.render_prompt(template.template, variables)
            logger.info(f"  → 步骤4: 渲染提示词完成 - 长度:{len(prompt)}字符")
            
            # 计算输入Token数量
            input_tokens = self.ai_manager.calculate_tokens(provider.name, prompt)
            logger.info(f"  → 步骤5: 计算Token完成 - 输入Token:{input_tokens}")
            
            # 创建分析记录
            analysis = self.db_service.create_analysis(
                novel_id=chapter.novel_id,
                chapter_id=chapter_id,
                prompt_template_id=prompt_template_id,
                ai_provider_id=ai_provider_id,
                model_name=model_name,
                analysis_type=template.category,
                input_text=prompt
            )
            logger.info(f"  → 步骤6: 创建分析记录完成 - 记录ID:{analysis.id}")
            
            # 更新分析状态为运行中
            self.db_service.update_analysis(analysis.id, 
                                          status='running',
                                          input_tokens=input_tokens)
            
            try:
                logger.info(f"  → 步骤7: 开始调用AI服务 - 模型:{model_name}")
                # 调用AI服务进行分析（同步版本）
                result = self.ai_manager.generate_analysis_sync(
                    provider.name, model_name, prompt
                )
                
                if result.get('success'):
                    # 分析成功
                    content = result.get('content', '')
                    output_tokens = self.ai_manager.calculate_tokens(provider.name, content)
                    estimated_cost = self.ai_manager.estimate_cost(
                        provider.name, input_tokens, output_tokens, model_name
                    )
                    
                    logger.info(f"  → 步骤8: AI分析完成 - 输出Token:{output_tokens}, 预估成本:{estimated_cost}")
                    
                    # 保存分析结果
                    self.db_service.update_analysis(analysis.id,
                                                  status='completed',
                                                  output_text=content,
                                                  output_tokens=output_tokens,
                                                  estimated_cost=estimated_cost,
                                                  total_tokens=input_tokens + output_tokens)
                    
                    logger.info(f"  → 步骤9: 保存分析结果完成 - 总Token:{input_tokens + output_tokens}")
                    
                    return {
                        'success': True,
                        'content': content,
                        'analysis_id': analysis.id,
                        'input_tokens': input_tokens,
                        'output_tokens': output_tokens,
                        'estimated_cost': estimated_cost
                    }
                else:
                    # 分析失败
                    logger.error(f"  → 步骤8: AI分析失败")
                    self.db_service.update_analysis(analysis.id,
                                                  status='failed',
                                                  error_message='AI分析失败')
                    return {
                        'success': False,
                        'error': 'AI分析失败',
                        'analysis_id': analysis.id
                    }
            
            except AIServiceException as e:
                # AI服务异常
                self.db_service.update_analysis(analysis.id,
                                              status='failed',
                                              error_message=str(e))
                return {
                    'success': False,
                    'error': str(e),
                    'analysis_id': analysis.id
                }
            
            except Exception as e:
                # 其他异常
                logger.error(f"分析章节时发生错误: {str(e)}\n堆栈信息:\n{traceback.format_exc()}")
                self.db_service.update_analysis(analysis.id,
                                              status='failed',
                                              error_message=str(e))
                return {
                    'success': False,
                    'error': str(e),
                    'analysis_id': analysis.id
                }
        
        except Exception as e:
            logger.error(f"  → 章节分析初始化失败: {str(e)}\n堆栈信息:\n{traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def create_batch_analysis_task(self, task_name: str, description: str,
                                       novel_id: int, chapter_ids: List[int],
                                       prompt_template_id: int, ai_provider_id: int,
                                       model_name: str) -> Dict[str, Any]:
        """创建批量分析任务"""
        try:
            # 创建分析任务
            task = self.db_service.create_analysis_task(
                task_name=task_name,
                description=description,
                novel_id=novel_id,
                chapter_ids=chapter_ids,
                prompt_template_id=prompt_template_id,
                ai_provider_id=ai_provider_id,
                model_name=model_name
            )
            
            return {
                'success': True,
                'task_id': task.id,
                'total_chapters': len(chapter_ids)
            }
        
        except Exception as e:
            logger.error(f"创建批量分析任务失败: {str(e)}\n堆栈信息:\n{traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_analysis_result(self, analysis_id: int) -> Dict[str, Any]:
        """获取分析结果"""
        try:
            analysis = self.db_service.get_analysis_by_id(analysis_id)
            if not analysis:
                return {
                    'success': False,
                    'error': '分析记录不存在'
                }
            
            return {
                'success': True,
                'analysis': {
                    'id': analysis.id,
                    'status': analysis.status,
                    'analysis_type': analysis.analysis_type,
                    'model_name': analysis.model_name,
                    'input_text': analysis.input_text,
                    'output_text': analysis.output_text,
                    'input_tokens': analysis.input_tokens,
                    'output_tokens': analysis.output_tokens,
                    'total_tokens': analysis.total_tokens,
                    'estimated_cost': analysis.estimated_cost,
                    'created_at': analysis.created_at.isoformat(),
                    'completed_at': analysis.completed_at.isoformat() if analysis.completed_at else None,
                    'error_message': analysis.error_message
                }
            }
        
        except Exception as e:
            logger.error(f"获取分析结果失败: {str(e)}\n堆栈信息:\n{traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_chapter_analyses(self, chapter_id: int) -> Dict[str, Any]:
        """获取章节的所有分析结果"""
        try:
            analyses = self.db_service.get_analyses_by_chapter(chapter_id)
            
            result_analyses = []
            for analysis in analyses:
                result_analyses.append({
                    'id': analysis.id,
                    'status': analysis.status,
                    'analysis_type': analysis.analysis_type,
                    'model_name': analysis.model_name,
                    'output_text': analysis.output_text,
                    'input_tokens': analysis.input_tokens,
                    'output_tokens': analysis.output_tokens,
                    'total_tokens': analysis.total_tokens,
                    'estimated_cost': analysis.estimated_cost,
                    'created_at': analysis.created_at.isoformat(),
                    'completed_at': analysis.completed_at.isoformat() if analysis.completed_at else None,
                    'error_message': analysis.error_message
                })
            
            return {
                'success': True,
                'analyses': result_analyses
            }
        
        except Exception as e:
            logger.error(f"获取章节分析结果失败: {str(e)}\n堆栈信息:\n{traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_novel_analyses(self, novel_id: int) -> Dict[str, Any]:
        """获取小说的所有分析结果"""
        try:
            analyses = self.db_service.get_analyses_by_novel(novel_id)
            
            # 按章节分组
            analyses_by_chapter = {}
            for analysis in analyses:
                chapter_id = analysis.chapter_id
                if chapter_id not in analyses_by_chapter:
                    analyses_by_chapter[chapter_id] = []
                
                analyses_by_chapter[chapter_id].append({
                    'id': analysis.id,
                    'status': analysis.status,
                    'analysis_type': analysis.analysis_type,
                    'model_name': analysis.model_name,
                    'output_text': analysis.output_text,
                    'input_tokens': analysis.input_tokens,
                    'output_tokens': analysis.output_tokens,
                    'total_tokens': analysis.total_tokens,
                    'estimated_cost': analysis.estimated_cost,
                    'created_at': analysis.created_at.isoformat(),
                    'completed_at': analysis.completed_at.isoformat() if analysis.completed_at else None,
                    'error_message': analysis.error_message
                })
            
            return {
                'success': True,
                'analyses_by_chapter': analyses_by_chapter
            }
        
        except Exception as e:
            logger.error(f"获取小说分析结果失败: {str(e)}\n堆栈信息:\n{traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_task_status(self, task_id: int) -> Dict[str, Any]:
        """获取任务状态"""
        try:
            task = self.db_service.get_analysis_task_by_id(task_id)
            if not task:
                return {
                    'success': False,
                    'error': '任务不存在'
                }
            
            # 从TaskChapterStatus表获取实时的章节状态统计
            chapter_statuses = self.db_service.get_task_chapter_statuses(
                task_id=task_id, 
                task_type='analysis'
            )
            
            # 实时计算完成和失败的章节数
            completed_chapters = len([s for s in chapter_statuses if s.status == 'completed'])
            failed_chapters = len([s for s in chapter_statuses if s.status == 'failed'])
            running_chapters = len([s for s in chapter_statuses if s.status == 'running'])
            pending_chapters = len([s for s in chapter_statuses if s.status == 'pending'])
            
            logger.info(f"任务 {task_id} 实时状态统计: 完成={completed_chapters}, 失败={failed_chapters}, 运行中={running_chapters}, 待处理={pending_chapters}, TaskChapterStatus记录总数={len(chapter_statuses)}")
            
            # 获取AI服务商信息，用于显示服务商名称而不是原始model_name
            ai_provider = self.db_service.get_ai_provider_by_id(task.ai_provider_id)
            provider_display_name = ai_provider.display_name if ai_provider else "未知服务商"
            model_display = f"{provider_display_name} / {task.model_name}"
            
            # 如果TaskChapterStatus表中有数据，使用实时计算的数据；否则使用任务表中的数据
            if chapter_statuses:
                actual_completed = completed_chapters
                actual_failed = failed_chapters
            else:
                # 如果没有TaskChapterStatus记录，则使用任务表中的原始数据
                actual_completed = task.completed_chapters
                actual_failed = task.failed_chapters
            
            return {
                'success': True,
                'task': {
                    'id': task.id,
                    'task_name': task.task_name,
                    'description': task.description,
                    'status': task.status,
                    'novel_id': task.novel_id,
                    'workflow_id': task.workflow_id,  # 添加workflow_id字段
                    'model_name': task.model_name,  # 原始模型名称，用于API调用
                    'model_display': model_display,  # 显示服务商/模型格式
                    'ai_provider_id': task.ai_provider_id,
                    'prompt_template_id': task.prompt_template_id,
                    'chapter_ids': task.chapter_ids,
                    'total_chapters': task.total_chapters,
                    'completed_chapters': actual_completed,  # 使用实时计算的数据
                    'failed_chapters': actual_failed,        # 使用实时计算的数据
                    'running_chapters': running_chapters,    # 新增运行中章节数
                    'pending_chapters': pending_chapters,    # 新增待处理章节数
                    'created_at': task.created_at.isoformat(),
                    'started_at': task.started_at.isoformat() if task.started_at else None,
                    'completed_at': task.completed_at.isoformat() if task.completed_at else None
                }
            }
        
        except Exception as e:
            logger.error(f"获取任务状态失败: {str(e)}\n堆栈信息:\n{traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }

# 全局分析服务实例
analysis_service = AnalysisService()