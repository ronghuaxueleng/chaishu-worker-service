"""
知识图谱配置服务
"""
import logging
import threading
from typing import Optional, Dict, Any
from ..models.database import db_manager, KnowledgeGraphConfig

logger = logging.getLogger(__name__)


class KnowledgeGraphConfigService:
    """知识图谱配置服务"""

    def __init__(self):
        self._default_config = None
        self._config_lock = threading.Lock()  # 防止并发初始化竞态

    def get_default_config(self) -> Optional[KnowledgeGraphConfig]:
        """获取默认配置（带缓存，双重检查锁定）"""
        # 第一次检查（无锁，快速路径）
        if self._default_config is not None:
            return self._default_config

        with self._config_lock:
            # 第二次检查（持锁，防止重复初始化）
            if self._default_config is not None:
                return self._default_config

            with db_manager.get_session() as session:
                try:
                    config = session.query(KnowledgeGraphConfig).filter_by(
                        is_default=True
                    ).first()

                    # 如果没有默认配置，创建一个基础配置
                    if not config:
                        logger.warning("未找到默认知识图谱配置，使用基础配置")
                        # 返回一个临时配置对象，不保存到数据库
                        class TempConfig:
                            def __init__(self):
                                self.ai_provider = None
                                self.ai_model = None
                                self.use_ai = False
                                self.max_content_length = 4000
                                self.enable_entity_extraction = True
                                self.enable_relation_extraction = True
                                self.entity_types = [
                                    {"type": "character", "name": "人物", "enabled": True},
                                    {"type": "location", "name": "地点", "enabled": True},
                                    {"type": "organization", "name": "组织", "enabled": True},
                                    {"type": "event", "name": "事件", "enabled": True}
                                ]
                                self.relation_types = [
                                    {"type": "FRIEND", "name": "朋友", "enabled": True},
                                    {"type": "ENEMY", "name": "敌人", "enabled": True},
                                    {"type": "LOVES", "name": "爱慕", "enabled": True},
                                    {"type": "HATES", "name": "仇恨", "enabled": True},
                                    {"type": "KNOWS", "name": "认识", "enabled": True},
                                    {"type": "LEADS", "name": "领导", "enabled": True},
                                    {"type": "FOLLOWS", "name": "跟随", "enabled": True},
                                    {"type": "PARTICIPATES_IN", "name": "参与", "enabled": True},
                                    {"type": "OCCURS_IN", "name": "发生于", "enabled": True}
                                ]
                                self.rule_config = {
                                    "character_patterns": [
                                        r'(?:道|说|叫|呼|唤|见|看|听)[道说]?"([一-龯]{2,4})"',
                                        r'"([一-龯]{2,4})"(?:道|说|叫|呼|问|答)',
                                        r'([一-龯]{2,4})(?:大师|先生|小姐|公子|少爷|姑娘)',
                                        r'(?:师父|师兄|师姐|师弟|师妹)([一-龯]{2,4})'
                                    ],
                                    "location_patterns": [
                                        r'(?:来到|到了|在)([一-龯]{2,6}(?:山|峰|谷|洞|城|镇|村|府|宫|殿|楼|阁|院|房|堂))',
                                        r'([一-龯]{2,6}(?:山|峰|谷|洞|城|镇|村|府|宫|殿|楼|阁|院|房|堂))(?:中|内|里|上|下)'
                                    ],
                                    "filter_words": ["什么", "这样", "那样", "如何", "怎么", "为何", "哪里", "这里", "那里"]
                                }

                        self._default_config = TempConfig()
                    else:
                        # 将 ORM 对象从 session 中安全分离（expunge），
                        # 使其可在 session 关闭后继续访问已加载的属性
                        session.expunge(config)
                        self._default_config = config
                except Exception as e:
                    session.rollback()
                    raise

        return self._default_config
    
    def get_config_by_id(self, config_id: int) -> Optional[KnowledgeGraphConfig]:
        """根据ID获取配置"""
        with db_manager.get_session() as session:
            try:
                return session.query(KnowledgeGraphConfig).filter_by(id=config_id).first()
            except Exception as e:
                session.rollback()
                raise
    
    def refresh_cache(self):
        """刷新缓存"""
        self._default_config = None
    
    def get_ai_config(self, config: Optional[KnowledgeGraphConfig] = None) -> Dict[str, Any]:
        """获取AI配置信息"""
        if config is None:
            config = self.get_default_config()
        
        # 基础配置（无默认配置时的兜底）
        if not config:
            result = {
                'use_ai': False,
                'provider_name': None,
                'model_name': None,
                'max_content_length': 4000
            }
        else:
            result = {
                'use_ai': bool(config.use_ai and bool(config.ai_provider)),
                'provider_name': config.ai_provider,
                'model_name': config.ai_model,
                'max_content_length': config.max_content_length
            }

        # Provider 运行时覆盖：支持按 Worker 进程选择不同的服务商
        # 由 kg_task_worker 在子进程内设置环境变量 `KG_ACTIVE_PROVIDER`
        try:
            import os
            provider_override = os.environ.get('KG_ACTIVE_PROVIDER')
            if provider_override:
                from ..services.database_service import db_service
                prov = db_service.get_ai_provider_by_name(provider_override)
                if prov and prov.is_active:
                    result['use_ai'] = True
                    result['provider_name'] = provider_override
                    # 若未配置模型或模型不存在，为该服务商选择第一个可用模型
                    model = result.get('model_name')
                    if not model or (prov.models and model not in prov.models):
                        result['model_name'] = (prov.models or [''])[0]
                    logger.debug(f"使用运行时Provider覆盖: {provider_override} / {result.get('model_name')}")
        except Exception:
            # 覆盖失败不影响正常配置
            pass

        return result
    
    def get_extraction_config(self, config: Optional[KnowledgeGraphConfig] = None) -> Dict[str, Any]:
        """获取提取配置信息"""
        if config is None:
            config = self.get_default_config()
        
        if not config:
            return {
                'enable_entity_extraction': True,
                'enable_relation_extraction': True,
                'enabled_entity_types': ['character', 'location', 'organization', 'event'],
                'enabled_relation_types': ['FRIEND', 'ENEMY', 'LOVES', 'HATES', 'KNOWS', 'LEADS', 'FOLLOWS', 'PARTICIPATES_IN', 'OCCURS_IN']
            }
        
        # 解析启用的实体类型
        enabled_entity_types = []
        if config.entity_types:
            for entity_type in config.entity_types:
                if entity_type.get('enabled', True):
                    enabled_entity_types.append(entity_type['type'])
        
        # 解析启用的关系类型
        enabled_relation_types = []
        if config.relation_types:
            for relation_type in config.relation_types:
                if relation_type.get('enabled', True):
                    enabled_relation_types.append(relation_type['type'])
        
        return {
            'enable_entity_extraction': config.enable_entity_extraction,
            'enable_relation_extraction': config.enable_relation_extraction,
            'enabled_entity_types': enabled_entity_types,
            'enabled_relation_types': enabled_relation_types
        }
    
    def get_rule_config(self, config: Optional[KnowledgeGraphConfig] = None) -> Dict[str, Any]:
        """获取规则配置信息"""
        if config is None:
            config = self.get_default_config()
        
        if not config or not config.rule_config:
            return {
                "character_patterns": [
                    r'(?:道|说|叫|呼|唤|见|看|听)[道说]?"([一-龯]{2,4})"',
                    r'"([一-龯]{2,4})"(?:道|说|叫|呼|问|答)',
                    r'([一-龯]{2,4})(?:大师|先生|小姐|公子|少爷|姑娘)',
                    r'(?:师父|师兄|师姐|师弟|师妹)([一-龯]{2,4})'
                ],
                "location_patterns": [
                    r'(?:来到|到了|在)([一-龯]{2,6}(?:山|峰|谷|洞|城|镇|村|府|宫|殿|楼|阁|院|房|堂))',
                    r'([一-龯]{2,6}(?:山|峰|谷|洞|城|镇|村|府|宫|殿|楼|阁|院|房|堂))(?:中|内|里|上|下)'
                ],
                "filter_words": ["什么", "这样", "那样", "如何", "怎么", "为何", "哪里", "这里", "那里"]
            }
        
        return config.rule_config


# 全局配置服务实例
kg_config_service = KnowledgeGraphConfigService()
