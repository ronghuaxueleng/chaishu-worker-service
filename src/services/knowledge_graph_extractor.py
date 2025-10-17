"""
知识图谱数据提取模块
从小说文本中自动提取人物、事件、地点、组织等信息
"""
import logging
import re
import json
import hashlib
import gc
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass
from ..models.database import db_manager, Novel, Chapter, Analysis
# 延迟导入，避免循环依赖问题
from ..ai.ai_service import get_ai_manager

logger = logging.getLogger(__name__)


@dataclass
class ExtractedEntity:
    """提取的实体"""
    name: str
    entity_type: str  # character, event, location, organization
    properties: Dict[str, Any]
    chapter_id: int
    novel_id: int


@dataclass
class ExtractedRelation:
    """提取的关系"""
    from_entity: str
    to_entity: str
    relation_type: str
    properties: Dict[str, Any]
    chapter_id: int
    novel_id: int


class KnowledgeGraphExtractor:
    """知识图谱提取器"""

    def __init__(self):
        """初始化提取器"""
        self.ai_manager = get_ai_manager()

        # 提取提示词模板
        self.extraction_prompts = {
            'entities': """请从以下小说章节中提取人物、地点、组织等实体信息。

章节标题：{title}
章节内容：{content}

请按以下JSON格式输出：
{{
    "characters": [
        {{
            "name": "人物名称",
            "description": "简短描述",
            "traits": ["性格特点1", "性格特点2"],
            "title": "称号或职位",
            "is_protagonist": false,
            "protagonist_score": 0
        }}
    ],
    "locations": [
        {{
            "name": "地点名称",
            "description": "地点描述",
            "type": "地点类型"
        }}
    ],
    "organizations": [
        {{
            "name": "组织名称",
            "description": "组织描述",
            "type": "组织类型"
        }}
    ],
    "events": [
        {{
            "name": "事件名称",
            "description": "事件描述",
            "importance": "重要程度",
            "participants": ["参与者1", "参与者2"]
        }}
    ]
}}

注意：
1. 对于人物，请根据其在章节中的重要性、出场频次、情节推动作用等因素，判断其可能是主角的概率
2. protagonist_score为0-100的整数，表示该人物是主角的可能性（100表示很可能是主角，0表示不太可能）
3. 只有protagonist_score >= 80的人物才标记is_protagonist为true
4. 只提取在本章节中明确出现的实体，确保信息准确。""",

            'protagonist_analysis': """请分析以下小说章节，重点识别可能的主角人物。

章节标题：{title}
章节内容：{content}

请按以下JSON格式输出主角分析结果：
{{
    "protagonist_candidates": [
        {{
            "name": "人物名称",
            "score": 85,
            "reasons": [
                "第一人称视角叙述",
                "大量内心独白",
                "情节围绕其展开",
                "其他人物对其反应强烈"
            ],
            "evidence": ["具体的文本证据1", "具体的文本证据2"]
        }}
    ],
    "narrative_perspective": "第一人称/第三人称有限/第三人称全知",
    "chapter_focus": "本章主要关注的人物和事件"
}}

评分标准（0-100分）：
- 90-100分：几乎确定是主角（第一人称视角，大量内心描写，情节核心）
- 80-89分：很可能是主角（重要视角人物，情节围绕其展开）
- 60-79分：可能是重要角色（有一定篇幅，但不确定是否为主角）
- 40-59分：配角（有名字有对话，但作用有限）
- 0-39分：龙套角色（仅仅提及或简单出场）""",

            'relationships': """请从以下小说章节中提取人物关系和事件关系。

章节标题：{title}
章节内容：{content}

请按以下JSON格式输出：
{{
    "character_relationships": [
        {{
            "from": "人物A",
            "to": "人物B",
            "relation": "FRIEND|ENEMY|LOVES|HATES|KNOWS|LEADS|FOLLOWS",
            "description": "关系描述",
            "strength": "强度(1-10)"
        }}
    ],
    "event_relationships": [
        {{
            "character": "人物名称",
            "event": "事件名称",
            "role": "参与角色",
            "description": "参与描述"
        }}
    ],
    "location_relationships": [
        {{
            "event": "事件名称",
            "location": "地点名称",
            "description": "位置关系描述"
        }}
    ]
}}

只提取在本章节中明确体现的关系。"""
        }

    def extract_from_chapter(self, chapter_id: int, use_ai: bool = True, config_id: int = None) -> Tuple[List[ExtractedEntity], List[ExtractedRelation]]:
        """从章节提取知识图谱数据"""
        try:
            # 获取章节数据
            session = db_manager.get_session()
            try:
                chapter = session.query(Chapter).filter_by(id=chapter_id).first()
                if not chapter:
                    logger.error(f"章节不存在: {chapter_id}")
                    return [], []

                logger.info(f"开始提取章节知识图谱: {chapter.title}")

                # 检查章节内容是否为空或过短
                if not chapter.content or len(chapter.content.strip()) < 10:
                    logger.warning(f"章节内容为空或过短，跳过提取: {chapter.title}")
                    return [], []

                # 获取配置
                from ..services.kg_config_service import kg_config_service
                config = None
                if config_id:
                    config = kg_config_service.get_config_by_id(config_id)
                if not config:
                    config = kg_config_service.get_default_config()

                entities = []
                relations = []

                if use_ai and self.ai_manager:
                    # 若Provider暂停，立刻中断，交由上层处理（避免将章节误标为失败）
                    try:
                        from ..services.kg_config_service import kg_config_service
                        ai_cfg_check = kg_config_service.get_ai_config(config)
                        from ..services.ai_provider_throttle import is_suspended as _is_suspended
                        if ai_cfg_check.get('provider_name') and _is_suspended(ai_cfg_check.get('provider_name')):
                            logger.warning(f"Provider 暂停中，跳过章节提取: provider={ai_cfg_check.get('provider_name')}, chapter_id={chapter_id}")
                            # 通过抛出特定异常通知上层逻辑暂停任务并回退章节状态
                            raise RuntimeError('provider_suspended')
                    except RuntimeError:
                        raise
                    except Exception:
                        # 覆盖失败时不影响正常流程
                        pass
                    # 使用AI提取
                    # 若模型未配置，尝试用provider的第一个模型作为默认模型
                    try:
                        from ..services.kg_config_service import kg_config_service
                        ai_cfg = kg_config_service.get_ai_config(config)
                        if not ai_cfg.get('model_name'):
                            from src.services.database_service import db_service
                            provider = db_service.get_ai_provider_by_name(ai_cfg.get('provider_name'))
                            if provider and provider.models:
                                # 动态为当前提取过程指定默认模型
                                config.ai_model = provider.models[0]
                    except Exception:
                        pass

                    ai_entities, ai_relations = self._extract_with_ai(chapter, config)
                    entities.extend(ai_entities)
                    relations.extend(ai_relations)
                    # 注意：不再回退到规则提取。AI为空即视为失败，由上层处理失败逻辑。
                else:
                    # 使用规则提取
                    rule_entities, rule_relations = self._extract_with_rules(chapter, config)
                    entities.extend(rule_entities)
                    relations.extend(rule_relations)

                logger.info(f"提取完成 - 实体: {len(entities)}, 关系: {len(relations)}")
                return entities, relations

            finally:
                session.close()

        except Exception as e:
            # 对 Provider 暂停的情况，向上抛出，交由上层处理（不要在这里吞掉）
            if 'provider_suspended' in str(e):
                raise
            logger.error(f"章节知识图谱提取失败: {e}")
            return [], []

    def _extract_with_ai(self, chapter: Chapter, config=None) -> Tuple[List[ExtractedEntity], List[ExtractedRelation]]:
        """使用AI提取知识图谱数据"""
        entities = []
        relations = []

        try:
            # 检查AI管理器是否可用
            if not self.ai_manager:
                logger.warning("AI管理器未初始化，跳过AI提取")
                return entities, relations

            # 获取配置
            from ..services.kg_config_service import kg_config_service
            ai_config = kg_config_service.get_ai_config(config)
            
            if not ai_config['use_ai'] or not ai_config['provider_name']:
                logger.info("AI提取未启用或未配置AI服务商，跳过AI提取")
                return entities, relations

            # 提取实体
            entity_prompt = self.extraction_prompts['entities'].format(
                title=chapter.title,
                content=chapter.content[:ai_config['max_content_length']]
            )

            entity_result = self.ai_manager.generate_response(
                prompt=entity_prompt,
                provider_name=ai_config['provider_name'],
                model_name=ai_config['model_name']
            )

            if entity_result and entity_result.get('success'):
                entity_data = self._parse_ai_response(entity_result['response'])
                if entity_data:
                    entities.extend(self._convert_ai_entities(entity_data, chapter, config))

            # 进行额外的主角分析（如果有人物实体）
            if any(e.entity_type == 'character' for e in entities):
                protagonist_data = self._analyze_protagonist(chapter, ai_config)
                if protagonist_data:
                    self._merge_protagonist_analysis(entities, protagonist_data)

            # 提取关系
            relation_prompt = self.extraction_prompts['relationships'].format(
                title=chapter.title,
                content=chapter.content[:ai_config['max_content_length']]
            )

            relation_result = self.ai_manager.generate_response(
                prompt=relation_prompt,
                provider_name=ai_config['provider_name'],
                model_name=ai_config['model_name']
            )

            if relation_result and relation_result.get('success'):
                relation_data = self._parse_ai_response(relation_result['response'])
                if relation_data:
                    relations.extend(self._convert_ai_relations(relation_data, chapter, config))

        except Exception as e:
            logger.error(f"AI提取失败: {e}")
            # 记录服务商失败次数用于节流/暂停
            try:
                from ..services.ai_provider_throttle import increment_failure
                provider_name = None
                try:
                    from ..services.kg_config_service import kg_config_service
                    provider_name = kg_config_service.get_ai_config(config).get('provider_name')
                except Exception:
                    provider_name = None
                if provider_name:
                    increment_failure(provider_name)
            except Exception:
                pass

        return entities, relations

    def _extract_with_rules(self, chapter: Chapter, config=None) -> Tuple[List[ExtractedEntity], List[ExtractedRelation]]:
        """使用规则提取知识图谱数据"""
        entities = []
        relations = []

        try:
            content = chapter.content
            
            # 获取规则配置
            from ..services.kg_config_service import kg_config_service
            rule_config = kg_config_service.get_rule_config(config)

            # 简单的人物名提取（中文姓名模式）
            character_patterns = rule_config.get('character_patterns', [
                r'(?:道|说|叫|呼|唤|见|看|听)[道说]?"([一-龯]{2,4})"',
                r'"([一-龯]{2,4})"(?:道|说|叫|呼|问|答)',
                r'([一-龯]{2,4})(?:大师|先生|小姐|公子|少爷|姑娘)',
                r'(?:师父|师兄|师姐|师弟|师妹)([一-龯]{2,4})'
            ])

            characters = set()
            for pattern in character_patterns:
                matches = re.findall(pattern, content)
                characters.update(matches)

            # 过滤常见词汇
            filter_words = set(rule_config.get('filter_words', [
                '什么', '这样', '那样', '如何', '怎么', '为何', '哪里', '这里', '那里'
            ]))
            characters = {char for char in characters if char not in filter_words and len(char) >= 2}

            # 创建人物实体
            for char_name in characters:
                entity = ExtractedEntity(
                    name=char_name,
                    entity_type='character',
                    properties={'extracted_by': 'rule', 'confidence': 0.7},
                    chapter_id=chapter.id,
                    novel_id=chapter.novel_id
                )
                entities.append(entity)

            # 简单的地点提取
            location_patterns = rule_config.get('location_patterns', [
                r'(?:来到|到了|在)([一-龯]{2,6}(?:山|峰|谷|洞|城|镇|村|府|宫|殿|楼|阁|院|房|堂))',
                r'([一-龯]{2,6}(?:山|峰|谷|洞|城|镇|村|府|宫|殿|楼|阁|院|房|堂))(?:中|内|里|上|下)'
            ])

            locations = set()
            for pattern in location_patterns:
                matches = re.findall(pattern, content)
                locations.update(matches)

            # 创建地点实体
            for loc_name in locations:
                entity = ExtractedEntity(
                    name=loc_name,
                    entity_type='location',
                    properties={'extracted_by': 'rule', 'confidence': 0.6},
                    chapter_id=chapter.id,
                    novel_id=chapter.novel_id
                )
                entities.append(entity)

        except Exception as e:
            logger.error(f"规则提取失败: {e}")

        return entities, relations

    def _parse_ai_response(self, response_text: str) -> Optional[Dict]:
        """解析AI响应的JSON数据"""
        try:
            # 尝试提取JSON部分
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1

            if json_start >= 0 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                return json.loads(json_str)

            # 如果没有找到JSON，尝试直接解析整个响应
            return json.loads(response_text)

        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}, 响应内容: {response_text[:500]}")
            return None

    def _convert_ai_entities(self, data: Dict, chapter: Chapter, config=None) -> List[ExtractedEntity]:
        """转换AI提取的实体数据"""
        entities = []

        try:
            # 转换人物
            for char_data in data.get('characters', []):
                entity = ExtractedEntity(
                    name=char_data['name'],
                    entity_type='character',
                    properties={
                        'description': char_data.get('description', ''),
                        'traits': char_data.get('traits', []),
                        'title': char_data.get('title', ''),
                        'is_protagonist': char_data.get('is_protagonist', False),
                        'protagonist_score': char_data.get('protagonist_score', 0),
                        'extracted_by': 'ai',
                        'confidence': 0.9
                    },
                    chapter_id=chapter.id,
                    novel_id=chapter.novel_id
                )
                entities.append(entity)

            # 转换地点
            for loc_data in data.get('locations', []):
                entity = ExtractedEntity(
                    name=loc_data['name'],
                    entity_type='location',
                    properties={
                        'description': loc_data.get('description', ''),
                        'type': loc_data.get('type', ''),
                        'extracted_by': 'ai',
                        'confidence': 0.9
                    },
                    chapter_id=chapter.id,
                    novel_id=chapter.novel_id
                )
                entities.append(entity)

            # 转换组织
            for org_data in data.get('organizations', []):
                entity = ExtractedEntity(
                    name=org_data['name'],
                    entity_type='organization',
                    properties={
                        'description': org_data.get('description', ''),
                        'type': org_data.get('type', ''),
                        'extracted_by': 'ai',
                        'confidence': 0.9
                    },
                    chapter_id=chapter.id,
                    novel_id=chapter.novel_id
                )
                entities.append(entity)

            # 转换事件
            for event_data in data.get('events', []):
                entity = ExtractedEntity(
                    name=event_data['name'],
                    entity_type='event',
                    properties={
                        'description': event_data.get('description', ''),
                        'importance': event_data.get('importance', ''),
                        'participants': event_data.get('participants', []),
                        'extracted_by': 'ai',
                        'confidence': 0.9
                    },
                    chapter_id=chapter.id,
                    novel_id=chapter.novel_id
                )
                entities.append(entity)

        except Exception as e:
            logger.error(f"转换AI实体数据失败: {e}")

        return entities

    def _convert_ai_relations(self, data: Dict, chapter: Chapter, config=None) -> List[ExtractedRelation]:
        """转换AI提取的关系数据"""
        relations = []

        try:
            # 转换人物关系
            for rel_data in data.get('character_relationships', []):
                relation = ExtractedRelation(
                    from_entity=rel_data['from'],
                    to_entity=rel_data['to'],
                    relation_type=rel_data['relation'],
                    properties={
                        'description': rel_data.get('description', ''),
                        'strength': rel_data.get('strength', ''),
                        'extracted_by': 'ai',
                        'confidence': 0.9
                    },
                    chapter_id=chapter.id,
                    novel_id=chapter.novel_id
                )
                relations.append(relation)

            # 转换事件关系
            for rel_data in data.get('event_relationships', []):
                relation = ExtractedRelation(
                    from_entity=rel_data['character'],
                    to_entity=rel_data['event'],
                    relation_type='PARTICIPATES_IN',
                    properties={
                        'role': rel_data.get('role', ''),
                        'description': rel_data.get('description', ''),
                        'extracted_by': 'ai',
                        'confidence': 0.9
                    },
                    chapter_id=chapter.id,
                    novel_id=chapter.novel_id
                )
                relations.append(relation)

            # 转换位置关系
            for rel_data in data.get('location_relationships', []):
                relation = ExtractedRelation(
                    from_entity=rel_data['event'],
                    to_entity=rel_data['location'],
                    relation_type='OCCURS_IN',
                    properties={
                        'description': rel_data.get('description', ''),
                        'extracted_by': 'ai',
                        'confidence': 0.9
                    },
                    chapter_id=chapter.id,
                    novel_id=chapter.novel_id
                )
                relations.append(relation)

        except Exception as e:
            logger.error(f"转换AI关系数据失败: {e}")

        return relations

    def create_knowledge_graph_task(self, novel_id: int, chapter_ids: List[int] = None,
                                   use_ai: bool = True, task_name: str = None) -> Dict:
        """创建知识图谱构建任务"""
        from ..services.knowledge_graph_task_service import kg_task_service

        return kg_task_service.create_task(
            novel_id=novel_id,
            task_name=task_name,
            chapter_ids=chapter_ids,
            use_ai=use_ai
        )

    def build_knowledge_graph_with_task(self, task_id: int) -> bool:
        """基于任务构建知识图谱（支持断点续传）"""
        from ..services.knowledge_graph_task_service import kg_task_service

        try:
            # 原子性地尝试启动任务（解决并发竞态条件）
            start_result = kg_task_service.try_start_task(task_id)
            if not start_result['success']:
                reason = start_result['reason']
                message = start_result['message']
                logger.warning(f"任务 {task_id} 启动失败: {reason} - {message}")

                # 对于已在运行的任务，返回True避免重复错误
                if reason == 'already_running':
                    return True
                else:
                    return False

            logger.info(f"任务 {task_id} 启动成功: {start_result['old_status']} -> {start_result['new_status']}")

            # 获取待处理的章节
            pending_chapter_ids = kg_task_service.get_pending_chapters(task_id)
            if not pending_chapter_ids:
                logger.info(f"任务 {task_id} 没有待处理的章节，检查任务完成状态")
                # 检查是否真正完成（所有章节都成功）
                if kg_task_service.is_task_fully_completed(task_id):
                    kg_task_service.update_task_status(task_id, 'completed')
                    logger.info(f"任务 {task_id} 已完成：所有章节都成功处理")
                else:
                    # 有失败的章节，标记任务为失败
                    completion_status = kg_task_service.get_task_completion_status(task_id)
                    kg_task_service.update_task_status(task_id, 'failed')
                    logger.warning(f"任务 {task_id} 标记为失败：未完成章节统计 {completion_status}")
                return True

            logger.info(f"开始处理任务 {task_id}, 待处理章节: {len(pending_chapter_ids)}")

            # 延迟导入，避免循环依赖
            from ..services.knowledge_graph_service import get_kg_service
            
            try:
                kg_service = get_kg_service()
            except Exception as e:
                logger.error(f"知识图谱服务初始化失败: {e}")
                kg_task_service.update_task_status(task_id, 'failed')
                return False

            # 获取任务信息
            task_info = kg_task_service.get_task(task_id)
            if not task_info:
                logger.error(f"任务信息不存在: {task_id}")
                kg_task_service.update_task_status(task_id, 'failed')
                return False

            # 获取任务配置
            novel_id = task_info['novel_id']
            use_ai = task_info['use_ai']

            session = db_manager.get_session()
            try:
                # 获取小说信息
                novel = session.query(Novel).filter_by(id=novel_id).first()
                if not novel:
                    logger.error(f"小说不存在: {novel_id}")
                    kg_task_service.update_task_status(task_id, 'failed')
                    return False

                # 创建小说节点
                kg_service.create_novel_node(novel.id, novel.title, novel.author)

                # 获取待处理的章节
                chapters = session.query(Chapter).filter(
                    Chapter.id.in_(pending_chapter_ids)
                ).all()

                # 分批处理章节
                batch_size = 5  # 缩小批次，便于断点续传
                total_entities = 0
                total_relations = 0

                for i, chapter in enumerate(chapters):
                    # 检查任务状态，支持暂停
                    current_task = kg_task_service.get_task(task_id)
                    if current_task and current_task['status'] == 'paused':
                        logger.info(f"任务 {task_id} 已暂停")
                        return True

                    try:
                        logger.debug(f"开始处理章节 {i+1}/{len(chapters)} - 小说: {novel.title}, 章节号 {chapter.chapter_number} (ID {chapter.id}), 标题: {chapter.title}")

                        # 在切换章节前检查 Provider 是否暂停：若暂停则回退并中止任务
                        if use_ai:
                            try:
                                from ..services.kg_config_service import kg_config_service
                                ai_cfg_loop = kg_config_service.get_ai_config(None)
                                from ..services.ai_provider_throttle import is_suspended as _is_suspended
                                if ai_cfg_loop.get('provider_name') and _is_suspended(ai_cfg_loop.get('provider_name')):
                                    logger.warning(f"Provider 暂停中，暂停任务 {task_id}，保留未处理章节: provider={ai_cfg_loop.get('provider_name')}")
                                    # 不更新章节为running，直接将任务置为paused并退出
                                    kg_task_service.update_task_status(task_id, 'paused')
                                    return True
                            except Exception:
                                pass

                        # 更新当前处理章节
                        kg_task_service.update_chapter_status(task_id, chapter.id, 'running')
                        logger.debug(f"小说: {novel.title}, 章节号 {chapter.chapter_number} (ID {chapter.id}) 状态已更新为 running")

                        # 创建章节节点
                        kg_service.create_chapter_node(
                            chapter.id, chapter.title, novel.id,
                            task_id=task_id,
                            chapter_number=chapter.chapter_number,
                            word_count=chapter.word_count,
                            content=chapter.content
                        )

                        # 提取实体和关系
                        try:
                            entities, relations = self.extract_from_chapter(chapter.id, use_ai)
                        except Exception as e:
                            if 'provider_suspended' in str(e):
                                # 回退章节状态为pending，不计入失败；暂停任务等待恢复
                                try:
                                    kg_task_service.update_chapter_status(task_id, chapter.id, 'pending')
                                except Exception:
                                    pass
                                kg_task_service.update_task_status(task_id, 'paused')
                                logger.info(f"Provider 暂停，已回退章节状态并暂停任务: task={task_id}, chapter_id={chapter.id}")
                                return True
                            else:
                                raise

                        # 若启用AI且无任何提取结果，则视为AI提取失败，不进行规则回退
                        if use_ai and (not entities and not relations):
                            logger.warning(
                                f"AI提取结果为空，标记章节失败（不回退规则） - 小说: {novel.title}, 章节号 {chapter.chapter_number} (ID {chapter.id})"
                            )
                            try:
                                from ..services.ai_provider_throttle import increment_failure
                                from ..services.kg_config_service import kg_config_service
                                # 尝试获取当前配置的服务商名称并记录失败
                                ai_cfg = kg_config_service.get_ai_config(None)
                                provider_name = ai_cfg.get('provider_name') if ai_cfg else None
                                if provider_name:
                                    increment_failure(provider_name)
                            except Exception:
                                pass

                            kg_task_service.update_chapter_status(
                                task_id, chapter.id, 'failed', 'AI提取结果为空（已按失败处理）'
                            )
                            continue

                        # 尝试创建实体和关系（可能失败）
                        neo4j_success = False
                        chapter_entities = 0
                        chapter_relations = 0

                        try:
                            chapter_entities, chapter_relations = self._create_entities_and_relations(
                                entities, relations, kg_service, task_id
                            )
                            neo4j_success = True
                            total_entities += chapter_entities
                            total_relations += chapter_relations
                            logger.debug(f"Neo4j数据创建成功: 小说: {novel.title}, 章节号 {chapter.chapter_number} (ID {chapter.id}) 标题: {chapter.title}, 实体: {chapter_entities}, 关系: {chapter_relations}")

                        except Exception as neo4j_e:
                            logger.error(f"Neo4j数据创建失败: 小说: {novel.title}, 章节号 {chapter.chapter_number} (ID {chapter.id}) 标题: {chapter.title}, 错误: {neo4j_e}")
                            neo4j_success = False

                        # 使用事务安全方法处理章节完成状态
                        status_updated = kg_task_service.process_chapter_with_transaction_safety(
                            task_id, chapter.id, chapter_entities, chapter_relations, neo4j_success
                        )

                        if status_updated:
                            if neo4j_success:
                                logger.debug(f"✓ 章节处理完成 - 小说: {novel.title}, 章节号 {chapter.chapter_number} (ID {chapter.id}) 标题: {chapter.title}, 实体: {chapter_entities}, 关系: {chapter_relations}")
                            else:
                                logger.warning(f"✗ 章节Neo4j处理失败 - 小说: {novel.title}, 章节号 {chapter.chapter_number} (ID {chapter.id}) 标题: {chapter.title}, 已标记为失败状态")

                            # 发送WebSocket进度更新事件
                            try:
                                logger.debug(f"准备推送任务进度到Redis: 任务{task_id}")
                                self._send_progress_update(task_id, task_info)
                            except Exception as e:
                                logger.error(f"发送进度更新事件失败: {e}", exc_info=True)

                        else:
                            logger.error(f"✗ 章节状态更新失败 - 小说: {novel.title}, 章节号 {chapter.chapter_number} (ID {chapter.id}) 标题: {chapter.title}")
                            # 即使状态更新失败，也继续处理下一个章节

                    except Exception as e:
                        logger.error(f"✗ 处理章节异常 - 小说: {novel.title}, 章节号 {chapter.chapter_number} (ID {chapter.id}) 标题: {chapter.title}, 错误: {e}")
                        kg_task_service.update_chapter_status(
                            task_id, chapter.id, 'failed', str(e)
                        )
                        continue

                    # 每处理5个章节进行一次垃圾回收
                    if (i + 1) % batch_size == 0:
                        gc.collect()
                        logger.info(f"小说: {novel.title} 已处理 {i+1} 个章节")

                # 检查是否所有章节都处理完成
                remaining_pending = kg_task_service.get_pending_chapters(task_id)
                if not remaining_pending:
                    # 检查是否真正完成（所有章节都成功）
                    if kg_task_service.is_task_fully_completed(task_id):
                        kg_task_service.update_task_status(
                            task_id, 'completed', total_entities, total_relations
                        )
                        logger.info(f"任务 {task_id} 完成，总实体: {total_entities}, 总关系: {total_relations}")
                    else:
                        # 有失败的章节，标记任务为失败
                        completion_status = kg_task_service.get_task_completion_status(task_id)
                        kg_task_service.update_task_status(task_id, 'failed')
                        logger.warning(f"任务 {task_id} 标记为失败：章节完成情况 {completion_status}")
                else:
                    logger.warning(f"任务 {task_id} 仍有 {len(remaining_pending)} 个章节待处理")

                return True

            finally:
                session.close()

        except Exception as e:
            logger.error(f"构建知识图谱任务失败: {e}")
            kg_task_service.update_task_status(task_id, 'failed')
            return False

    def _create_entities_and_relations(self, entities: List, relations: List, kg_service, task_id: int = None) -> Tuple[int, int]:
        """创建实体和关系，返回创建的数量"""
        from ..services.neo4j_failed_data_service import neo4j_failed_data_service

        entity_count = 0
        relation_count = 0

        # 创建实体
        for entity in entities:
            try:
                if entity.entity_type == 'character':
                    kg_service.create_character(entity.name, entity.novel_id, task_id=task_id, **entity.properties)
                elif entity.entity_type == 'location':
                    kg_service.create_location(entity.name, entity.novel_id, task_id=task_id, **entity.properties)
                elif entity.entity_type == 'organization':
                    kg_service.create_organization(entity.name, entity.novel_id, task_id=task_id, **entity.properties)
                elif entity.entity_type == 'event':
                    name_hash = hashlib.md5(entity.name.encode('utf-8')).hexdigest()[:8]
                    event_id = f"{entity.novel_id}_{entity.chapter_id}_{name_hash}"
                    kg_service.create_event(event_id, entity.name, entity.chapter_id, entity.novel_id, task_id=task_id, **entity.properties)
                entity_count += 1
            except Exception as e:
                logger.error(f"创建实体失败: {entity.name}, 错误: {e}")
                # 保存失败的实体到Redis
                entity_data = {
                    'type': entity.entity_type,
                    'name': entity.name,
                    'novel_id': entity.novel_id,
                    'chapter_id': getattr(entity, 'chapter_id', None),
                    'task_id': task_id,
                    'properties': entity.properties
                }
                neo4j_failed_data_service.save_failed_entity_to_redis(entity_data, str(e))

        # 创建关系
        for relation in relations:
            try:
                relation_type = relation.relation_type
                if relation_type in ['FRIEND', 'ENEMY', 'LOVES', 'HATES', 'KNOWS', 'LEADS', 'FOLLOWS']:
                    kg_service.character_relationship(
                        relation.from_entity, relation.to_entity, relation_type,
                        relation.novel_id, **relation.properties
                    )
                elif relation_type == 'PARTICIPATES_IN':
                    name_hash = hashlib.md5(relation.to_entity.encode('utf-8')).hexdigest()[:8]
                    event_id = f"{relation.novel_id}_{relation.chapter_id}_{name_hash}"
                    kg_service.character_participates_in_event(
                        relation.from_entity, event_id, relation.novel_id, **relation.properties
                    )
                elif relation_type == 'OCCURS_IN':
                    name_hash = hashlib.md5(relation.from_entity.encode('utf-8')).hexdigest()[:8]
                    event_id = f"{relation.novel_id}_{relation.chapter_id}_{name_hash}"
                    kg_service.event_occurs_in_location(
                        event_id, relation.to_entity, relation.novel_id, **relation.properties
                    )
                relation_count += 1
            except Exception as e:
                logger.error(f"创建关系失败: {relation.from_entity} -> {relation.to_entity}, 错误: {e}")
                # 保存失败的关系到Redis
                relation_data = {
                    'type': relation_type,
                    'from_entity': relation.from_entity,
                    'to_entity': relation.to_entity,
                    'novel_id': relation.novel_id,
                    'chapter_id': getattr(relation, 'chapter_id', None),
                    'task_id': task_id,
                    'properties': relation.properties
                }
                neo4j_failed_data_service.save_failed_relation_to_redis(relation_data, str(e))

        return entity_count, relation_count

    def _send_progress_update(self, task_id: int, task_info: Dict):
        """发送进度更新（通过 Redis 转发到主进程统一推送），避免直接依赖 Flask 上下文"""
        try:
            # 获取最新的任务信息
            from ..services.knowledge_graph_task_service import kg_task_service
            updated_task = kg_task_service.get_task(task_id)
            if not updated_task:
                return

            # 计算进度
            progress = 0
            if updated_task['total_chapters'] > 0:
                progress = round((updated_task['completed_chapters'] / updated_task['total_chapters']) * 100, 1)

            # 仅经由 Redis Pub/Sub 转发到主进程，再由主进程统一 emit 到 SocketIO
            try:
                from src.utils.redis_client import get_redis_client
                from src.services.chapter_write_worker import PROGRESS_CHANNEL
                redis_client = get_redis_client()
            except Exception:
                redis_client = None

            if redis_client:
                event_data = {
                    'type': 'kg_task_progress',
                    'task_id': task_id,
                    'status': updated_task['status'],
                    'progress': progress,
                    'completed_chapters': updated_task['completed_chapters'],
                    'failed_chapters': updated_task['failed_chapters'],
                    'total_chapters': updated_task['total_chapters'],
                    'updated_at': updated_task['updated_at']
                }
                redis_client.publish(PROGRESS_CHANNEL, event_data)
                # 降低日志级别为调试，避免刷屏
                logger.debug(f"已发布任务进度事件到Redis: 任务{task_id}, 进度{progress}%")

        except Exception as e:
            # 保守处理：不因进度推送失败影响任务主流程
            logger.debug(f"进度推送跳过/失败（不影响主流程）: {e}")

    def build_knowledge_graph(self, novel_id: int, chapter_ids: List[int] = None, use_ai: bool = True) -> bool:
        """构建小说知识图谱"""
        try:
            # 延迟导入，避免循环依赖
            from ..services.knowledge_graph_service import get_kg_service

            try:
                kg_service = get_kg_service()
            except Exception as e:
                logger.error(f"知识图谱服务初始化失败: {e}")
                return False

            session = db_manager.get_session()
            try:
                # 获取小说信息
                novel = session.query(Novel).filter_by(id=novel_id).first()
                if not novel:
                    logger.error(f"小说不存在: {novel_id}")
                    return False

                # 创建小说节点
                kg_service.create_novel_node(novel.id, novel.title, novel.author)

                # 获取要处理的章节
                if chapter_ids:
                    chapters = session.query(Chapter).filter(
                        Chapter.novel_id == novel_id,
                        Chapter.id.in_(chapter_ids)
                    ).all()
                else:
                    chapters = session.query(Chapter).filter_by(novel_id=novel_id).all()

                logger.info(f"开始构建知识图谱，小说: {novel.title}, 章节数: {len(chapters)}")

                # 存储所有提取的实体和关系
                all_entities = []
                all_relations = []

                # 分批处理章节，避免内存问题
                batch_size = 10  # 每批处理10个章节
                total_batches = (len(chapters) + batch_size - 1) // batch_size

                for batch_idx in range(0, len(chapters), batch_size):
                    batch_chapters = chapters[batch_idx:batch_idx + batch_size]
                    current_batch = batch_idx // batch_size + 1
                    logger.info(f"处理第 {current_batch}/{total_batches} 批章节 ({len(batch_chapters)}个)")

                    # 逐章节提取
                    for chapter in batch_chapters:
                        # 创建章节节点
                        kg_service.create_chapter_node(
                            chapter.id,
                            chapter.title,
                            novel.id,
                            chapter_number=chapter.chapter_number,
                            word_count=chapter.word_count,
                            content=chapter.content
                        )

                        # 提取实体和关系
                        entities, relations = self.extract_from_chapter(chapter.id, use_ai)
                        all_entities.extend(entities)
                        all_relations.extend(relations)

                    # 批次间强制垃圾回收，释放内存
                    gc.collect()
                    logger.info(f"第 {current_batch} 批处理完成，已处理 {len(all_entities)} 个实体")

                # 创建实体节点并建立基础关系
                entity_stats = {'character': 0, 'location': 0, 'organization': 0, 'event': 0}
                character_chapter_map = {}  # 记录人物在哪些章节出现

                for entity in all_entities:
                    try:
                        if entity.entity_type == 'character':
                            kg_service.create_character(entity.name, entity.novel_id, **entity.properties)
                            entity_stats['character'] += 1

                            # 记录人物出现的章节
                            if entity.name not in character_chapter_map:
                                character_chapter_map[entity.name] = set()
                            character_chapter_map[entity.name].add(entity.chapter_id)

                        elif entity.entity_type == 'location':
                            kg_service.create_location(entity.name, entity.novel_id, **entity.properties)
                            entity_stats['location'] += 1
                        elif entity.entity_type == 'organization':
                            kg_service.create_organization(entity.name, entity.novel_id, **entity.properties)
                            entity_stats['organization'] += 1
                        elif entity.entity_type == 'event':
                            # 使用更稳定的ID生成方式
                            name_hash = hashlib.md5(entity.name.encode('utf-8')).hexdigest()[:8]
                            event_id = f"{entity.novel_id}_{entity.chapter_id}_{name_hash}"
                            kg_service.create_event(event_id, entity.name, entity.chapter_id, entity.novel_id, **entity.properties)
                            entity_stats['event'] += 1
                    except Exception as e:
                        logger.error(f"创建实体失败: {entity.name}, 错误: {e}")

                # 创建人物出现在章节的关系
                for character_name, chapter_ids in character_chapter_map.items():
                    for chapter_id in chapter_ids:
                        try:
                            kg_service.character_appears_in_chapter(character_name, chapter_id, novel_id)
                        except Exception as e:
                            logger.error(f"创建人物章节关系失败: {character_name} -> 章节ID {chapter_id}, 错误: {e}")

                # 创建关系
                relation_stats = {}
                for relation in all_relations:
                    try:
                        relation_type = relation.relation_type

                        if relation_type in ['FRIEND', 'ENEMY', 'LOVES', 'HATES', 'KNOWS', 'LEADS', 'FOLLOWS']:
                            # 人物关系
                            kg_service.character_relationship(
                                relation.from_entity,
                                relation.to_entity,
                                relation_type,
                                relation.novel_id,
                                **relation.properties
                            )
                        elif relation_type == 'PARTICIPATES_IN':
                            # 人物参与事件
                            name_hash = hashlib.md5(relation.to_entity.encode('utf-8')).hexdigest()[:8]
                            event_id = f"{relation.novel_id}_{relation.chapter_id}_{name_hash}"
                            kg_service.character_participates_in_event(
                                relation.from_entity,
                                event_id,
                                relation.novel_id,
                                **relation.properties
                            )
                        elif relation_type == 'OCCURS_IN':
                            # 事件发生在地点
                            name_hash = hashlib.md5(relation.from_entity.encode('utf-8')).hexdigest()[:8]
                            event_id = f"{relation.novel_id}_{relation.chapter_id}_{name_hash}"
                            kg_service.event_occurs_in_location(
                                event_id,
                                relation.to_entity,
                                relation.novel_id,
                                **relation.properties
                            )

                        relation_stats[relation_type] = relation_stats.get(relation_type, 0) + 1
                    except Exception as e:
                        logger.error(f"创建关系失败: {relation.from_entity} -> {relation.to_entity}, 错误: {e}")

                logger.info(f"知识图谱构建完成")
                logger.info(f"实体统计: {entity_stats}")
                logger.info(f"关系统计: {relation_stats}")
                return True

            finally:
                session.close()

        except Exception as e:
            logger.error(f"构建知识图谱失败: {e}")
            return False

    def _analyze_protagonist(self, chapter: Chapter, ai_config: Dict) -> Optional[Dict]:
        """使用AI进行主角分析"""
        try:
            if not self.ai_manager:
                return None

            # 构建主角分析提示词
            protagonist_prompt = self.extraction_prompts['protagonist_analysis'].format(
                title=chapter.title,
                content=chapter.content[:ai_config['max_content_length']]
            )

            # 调用AI分析
            result = self.ai_manager.generate_response(
                prompt=protagonist_prompt,
                provider_name=ai_config['provider_name'],
                model_name=ai_config['model_name']
            )

            if result and result.get('success'):
                return self._parse_ai_response(result['response'])

        except Exception as e:
            logger.error(f"主角分析失败: {e}")

        return None

    def _merge_protagonist_analysis(self, entities: List[ExtractedEntity], protagonist_data: Dict):
        """将主角分析结果合并到实体中"""
        try:
            protagonist_candidates = protagonist_data.get('protagonist_candidates', [])
            
            # 为每个主角候选人更新对应的人物实体
            for candidate in protagonist_candidates:
                candidate_name = candidate.get('name', '')
                candidate_score = candidate.get('score', 0)
                
                # 找到对应的人物实体
                for entity in entities:
                    if (entity.entity_type == 'character' and 
                        entity.name == candidate_name):
                        
                        # 更新主角相关属性
                        current_score = entity.properties.get('protagonist_score', 0)
                        # 取最高分数
                        if candidate_score > current_score:
                            entity.properties['protagonist_score'] = candidate_score
                            entity.properties['is_protagonist'] = candidate_score >= 80
                            entity.properties['protagonist_reasons'] = candidate.get('reasons', [])
                            entity.properties['protagonist_evidence'] = candidate.get('evidence', [])
                        break
            
            # 记录章节的叙述视角信息
            narrative_perspective = protagonist_data.get('narrative_perspective', '')
            if narrative_perspective:
                logger.info(f"章节ID {entities[0].chapter_id if entities else 'unknown'} 叙述视角: {narrative_perspective}")

        except Exception as e:
            logger.error(f"合并主角分析结果失败: {e}")

    def get_novel_protagonists(self, novel_id: int) -> List[Dict]:
        """获取小说的主角列表（基于所有章节的分析结果）"""
        try:
            from ..services.knowledge_graph_service import get_kg_service
            kg_service = get_kg_service()
            
            if not kg_service:
                return []

            with kg_service.driver.session() as session:
                # 查询所有被标记为主角的人物
                query = """
                MATCH (c:Character {novel_id: $novel_id})
                WHERE c.is_protagonist = true OR c.protagonist_score >= 80
                RETURN c.name as name, 
                       c.protagonist_score as score,
                       c.description as description,
                       c.protagonist_reasons as reasons,
                       c.traits as traits
                ORDER BY c.protagonist_score DESC
                """
                
                result = session.run(query, novel_id=novel_id)
                
                protagonists = []
                for record in result:
                    protagonists.append({
                        'name': record['name'],
                        'score': record['score'] or 0,
                        'description': record['description'] or '',
                        'reasons': record['reasons'] or [],
                        'traits': record['traits'] or []
                    })
                
                return protagonists

        except Exception as e:
            logger.error(f"获取小说主角失败: {e}")
            return []


# 惰性加载知识图谱提取器实例
_kg_extractor = None

def get_kg_extractor():
    """获取知识图谱提取器实例（惰性加载）"""
    global _kg_extractor
    if _kg_extractor is None:
        try:
            _kg_extractor = KnowledgeGraphExtractor()
        except Exception as e:
            logger.error(f"知识图谱提取器初始化失败: {e}")
            _kg_extractor = None
    return _kg_extractor

# 向后兼容的属性访问
kg_extractor = property(lambda self: get_kg_extractor())
