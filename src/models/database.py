from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, ForeignKey, Float, JSON, Index, text, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime, timezone, timedelta
import os
import json
import base64
import logging

logger = logging.getLogger(__name__)

# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))

def beijing_now():
    """获取北京时间"""
    return datetime.now(BEIJING_TZ).replace(tzinfo=None)

Base = declarative_base()

class Novel(Base):
    __tablename__ = 'novels'
    __table_args__ = (
        Index('idx_novels_title', 'title'),
        Index('idx_novels_author', 'author'),
        Index('idx_novels_created_at', 'created_at'),
        Index('idx_novels_updated_at', 'updated_at'),
        Index('idx_novels_total_chapters', 'total_chapters'),
        Index('idx_novels_total_word_count', 'total_word_count'),
    )

    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    author = Column(String(255))
    description = Column(Text)
    file_path = Column(String(500))
    file_size = Column(Integer)
    total_chapters = Column(Integer, default=0)
    total_word_count = Column(Integer, default=0)  # 总字数
    tags = Column(JSON, default=list)  # 标签列表，存储为JSON数组
    is_deleting = Column(Integer, default=0)  # 是否正在删除: 0-否, 1-是
    chapters_parsed = Column(Integer, default=0)  # 是否已解析章节列表: 0-否, 1-是
    created_at = Column(DateTime, default=beijing_now)
    updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)

    # 关联关系
    chapters = relationship("Chapter", back_populates="novel", cascade="all, delete-orphan")
    analyses = relationship("Analysis", back_populates="novel", cascade="all, delete-orphan")

class Chapter(Base):
    __tablename__ = 'chapters'
    __table_args__ = (
        Index('idx_chapters_novel_id', 'novel_id'),
        Index('idx_chapters_novel_chapter_number', 'novel_id', 'chapter_number'),
        Index('idx_chapters_created_at', 'created_at'),
        Index('idx_chapters_word_count', 'word_count'),
    )
    
    id = Column(Integer, primary_key=True)
    novel_id = Column(Integer, ForeignKey('novels.id'), nullable=False)
    title = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)
    chapter_number = Column(Integer, nullable=False)
    word_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=beijing_now)
    
    # 关联关系
    novel = relationship("Novel", back_populates="chapters")
    analyses = relationship("Analysis", back_populates="chapter", cascade="all, delete-orphan")

class PromptTemplate(Base):
    __tablename__ = 'prompt_templates'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    category = Column(String(100), nullable=False)
    description = Column(Text)
    template = Column(Text, nullable=False)
    variables = Column(JSON)  # 存储模板变量
    variable_values = Column(JSON)  # 存储变量的默认值配置
    is_default = Column(Boolean, default=False)
    is_system_category = Column(Boolean, default=False)  # 标识是否为系统分类
    created_at = Column(DateTime, default=beijing_now)
    updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)
    
    # 关联关系
    analyses = relationship("Analysis", back_populates="prompt_template")

class AIProvider(Base):
    __tablename__ = 'ai_providers'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)  # openai, claude, zhipu, deepseek
    display_name = Column(String(100), nullable=False)
    api_key = Column(String(255))
    base_url = Column(String(255))
    models = Column(JSON)  # 可用模型列表
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=beijing_now)
    updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)
    
    # 关联关系
    analyses = relationship("Analysis", back_populates="ai_provider")

class Analysis(Base):
    __tablename__ = 'analyses'
    __table_args__ = (
        Index('idx_analyses_chapter_id', 'chapter_id'),
        Index('idx_analyses_novel_id', 'novel_id'),
        Index('idx_analyses_status', 'status'),
        Index('idx_analyses_created_at', 'created_at'),
        Index('idx_analyses_chapter_status', 'chapter_id', 'status'),
        Index('idx_analyses_novel_chapter', 'novel_id', 'chapter_id'),
        Index('idx_analyses_task_id', 'task_id'),
    )
    
    id = Column(Integer, primary_key=True)
    novel_id = Column(Integer, ForeignKey('novels.id'), nullable=False)
    chapter_id = Column(Integer, ForeignKey('chapters.id'), nullable=False)
    prompt_template_id = Column(Integer, ForeignKey('prompt_templates.id'), nullable=False)
    ai_provider_id = Column(Integer, ForeignKey('ai_providers.id'), nullable=False)
    task_id = Column(Integer, ForeignKey('analysis_tasks.id'))  # 关联的分析任务ID
    
    model_name = Column(String(100), nullable=False)
    analysis_type = Column(String(50), nullable=False)  # 剧情摘要, 章节大纲, 角色分析等
    
    # 输入输出
    input_text = Column(Text, nullable=False)
    output_text = Column(Text)
    
    # 状态和统计
    status = Column(String(20), default='pending')  # pending, running, completed, failed
    error_message = Column(Text)
    
    # Token统计
    input_tokens = Column(Integer, default=0)
    output_tokens = Column(Integer, default=0)
    total_tokens = Column(Integer, default=0)
    estimated_cost = Column(Float, default=0.0)
    
    # 时间戳
    created_at = Column(DateTime, default=beijing_now)
    completed_at = Column(DateTime)
    
    # 关联关系
    novel = relationship("Novel", back_populates="analyses")
    chapter = relationship("Chapter", back_populates="analyses")
    prompt_template = relationship("PromptTemplate", back_populates="analyses")
    ai_provider = relationship("AIProvider", back_populates="analyses")

class AnalysisTask(Base):
    __tablename__ = 'analysis_tasks'
    __table_args__ = (
        Index('idx_analysis_tasks_novel_id', 'novel_id'),
        Index('idx_analysis_tasks_status', 'status'),
        Index('idx_analysis_tasks_created_at', 'created_at'),
        Index('idx_analysis_tasks_novel_status', 'novel_id', 'status'),
        Index('idx_analysis_tasks_workflow_id', 'workflow_id'),
    )
    
    id = Column(Integer, primary_key=True)
    task_name = Column(String(255), nullable=False)
    description = Column(Text)
    status = Column(String(20), default='created')  # created, running, completed, failed
    
    # 任务配置
    novel_id = Column(Integer, ForeignKey('novels.id'), nullable=False)
    chapter_ids = Column(JSON)  # 要分析的章节ID列表
    prompt_template_id = Column(Integer, ForeignKey('prompt_templates.id'), nullable=False)
    ai_provider_id = Column(Integer, ForeignKey('ai_providers.id'), nullable=False)
    model_name = Column(String(100), nullable=False)
    workflow_id = Column(Integer, ForeignKey('workflows.id'))  # 关联的工作流ID
    
    # 进度统计
    total_chapters = Column(Integer, default=0)
    completed_chapters = Column(Integer, default=0)
    failed_chapters = Column(Integer, default=0)
    current_chapter_id = Column(Integer)  # 当前正在处理的章节ID
    
    # 时间戳
    created_at = Column(DateTime, default=beijing_now)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    
    # 关联关系
    novel = relationship("Novel")
    prompt_template = relationship("PromptTemplate")
    ai_provider = relationship("AIProvider")
    workflow = relationship("Workflow")

class Workflow(Base):
    __tablename__ = 'workflows'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    category = Column(String(100), nullable=False)  # analysis, creation, review
    is_active = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=beijing_now)
    updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)
    
    # 关联关系
    steps = relationship("WorkflowStep", back_populates="workflow", cascade="all, delete-orphan")
    executions = relationship("WorkflowExecution", back_populates="workflow", cascade="all, delete-orphan")

class WorkflowStep(Base):
    __tablename__ = 'workflow_steps'
    
    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflows.id'), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    template_name = Column(String(255), nullable=False)  # 引用的提示词模板名称
    prompt_template_id = Column(Integer, ForeignKey('prompt_templates.id'))  # 关联到 prompt_templates 表
    input_source = Column(String(50), nullable=False)  # original, previous, custom
    order_index = Column(Integer, nullable=False)
    is_optional = Column(Boolean, default=False)
    scope = Column(String(20), default='chapter')  # 步骤作用域：'chapter' 或 'global'
    
    # AI提供商和模型信息
    ai_provider_id = Column(Integer)  # AI提供商ID
    model_name = Column(String(100))  # 模型名称
    provider_name = Column(String(255))  # 提供商名称
    
    # 变量映射配置
    variable_mapping = Column(Text)  # JSON格式存储变量映射关系
    
    # 关联关系
    workflow = relationship("Workflow", back_populates="steps")
    prompt_template = relationship("PromptTemplate")  # 新增关联关系
    executions = relationship("WorkflowStepExecution", back_populates="step", cascade="all, delete-orphan")

class WorkflowExecution(Base):
    __tablename__ = 'workflow_executions'
    
    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflows.id'), nullable=False)
    novel_id = Column(Integer, ForeignKey('novels.id'), nullable=False)
    chapter_id = Column(Integer, ForeignKey('chapters.id'))  # 可选，如果是针对特定章节
    task_id = Column(Integer, ForeignKey('analysis_tasks.id'))  # 关联的分析任务ID
    
    status = Column(String(20), default='created')  # created, pending, running, completed, failed, cancelled, stopped
    current_step = Column(Integer, default=0)
    progress_text = Column(String(500))  # 当前进度描述
    error_message = Column(Text)
    
    # 输入输出
    initial_input = Column(Text)  # 初始输入内容
    final_output = Column(Text)   # 最终输出结果
    
    # 时间戳
    created_at = Column(DateTime, default=beijing_now)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    
    # 关联关系
    workflow = relationship("Workflow", back_populates="executions")
    novel = relationship("Novel")
    chapter = relationship("Chapter")
    step_executions = relationship("WorkflowStepExecution", back_populates="execution", cascade="all, delete-orphan")

class WorkflowStepExecution(Base):
    __tablename__ = 'workflow_step_executions'
    
    id = Column(Integer, primary_key=True)
    execution_id = Column(Integer, ForeignKey('workflow_executions.id'), nullable=False)
    step_id = Column(Integer, ForeignKey('workflow_steps.id'), nullable=False)
    prompt_template_id = Column(Integer, ForeignKey('prompt_templates.id'), nullable=False)
    ai_provider_id = Column(Integer, ForeignKey('ai_providers.id'))
    
    status = Column(String(20), default='pending')  # pending, running, completed, failed, skipped
    
    # 输入输出
    input_text = Column(Text)
    output_text = Column(Text)
    prompt_variables = Column(JSON)  # 用于渲染提示词的变量
    
    # AI调用信息
    model_name = Column(String(100))
    input_tokens = Column(Integer, default=0)
    output_tokens = Column(Integer, default=0)
    estimated_cost = Column(Float, default=0.0)
    
    # 时间戳
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    error_message = Column(Text)
    
    # 关联关系
    execution = relationship("WorkflowExecution", back_populates="step_executions")
    step = relationship("WorkflowStep", back_populates="executions")
    prompt_template = relationship("PromptTemplate")
    ai_provider = relationship("AIProvider")

class KnowledgeGraphTask(Base):
    __tablename__ = 'knowledge_graph_tasks'
    __table_args__ = (
        Index('idx_kg_tasks_novel_id', 'novel_id'),
        Index('idx_kg_tasks_status', 'status'),
        Index('idx_kg_tasks_created_at', 'created_at'),
    )

    id = Column(Integer, primary_key=True)
    task_name = Column(String(255), nullable=False)
    novel_id = Column(Integer, ForeignKey('novels.id'), nullable=False)

    # 任务配置
    chapter_ids = Column(JSON)  # 要处理的章节ID列表，如果为空则处理全部章节
    use_ai = Column(Boolean, default=True)  # 是否使用AI提取

    # 任务状态
    status = Column(String(20), default='created')  # created, running, paused, completed, failed, cancelled

    # 进度统计
    total_chapters = Column(Integer, default=0)
    completed_chapters = Column(Integer, default=0)
    failed_chapters = Column(Integer, default=0)
    skipped_chapters = Column(Integer, default=0)  # 跳过的章节（内容为空等）
    current_chapter_id = Column(Integer)  # 当前正在处理的章节ID

    # 实体统计
    total_entities = Column(Integer, default=0)
    total_relations = Column(Integer, default=0)

    # 错误信息
    error_message = Column(Text)
    last_error_chapter_id = Column(Integer)  # 最后出错的章节ID

    # 自动重试配置
    auto_retry_enabled = Column(Boolean, default=False)  # 是否启用自动重试
    retry_interval_minutes = Column(Integer, default=10)  # 重试间隔(分钟)，默认10分钟
    failed_at = Column(DateTime)  # 任务失败时间
    retry_scheduled_at = Column(DateTime)  # 计划重试时间
    retry_count = Column(Integer, default=0)  # 已重试次数

    # 时间戳
    created_at = Column(DateTime, default=beijing_now)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    paused_at = Column(DateTime)
    updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)

    # 关联关系
    novel = relationship("Novel")
    chapter_statuses = relationship("KnowledgeGraphChapterStatus", cascade="all, delete-orphan")

class KnowledgeGraphChapterStatus(Base):
    __tablename__ = 'knowledge_graph_chapter_status'
    __table_args__ = (
        Index('idx_kg_chapter_status_task', 'kg_task_id'),
        Index('idx_kg_chapter_status_chapter', 'chapter_id'),
        Index('idx_kg_chapter_status_status', 'status'),
        Index('idx_kg_chapter_status_task_chapter', 'kg_task_id', 'chapter_id'),
    )

    id = Column(Integer, primary_key=True)
    kg_task_id = Column(Integer, ForeignKey('knowledge_graph_tasks.id'), nullable=False)
    chapter_id = Column(Integer, ForeignKey('chapters.id'), nullable=False)
    status = Column(String(20), nullable=False)  # pending, running, completed, failed, skipped

    # 处理结果统计
    entities_extracted = Column(Integer, default=0)  # 提取的实体数量
    relations_extracted = Column(Integer, default=0)  # 提取的关系数量

    # 错误信息
    error_message = Column(Text)

    # 时间戳
    created_at = Column(DateTime, default=beijing_now)
    updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)
    started_at = Column(DateTime)  # 开始处理时间
    completed_at = Column(DateTime)  # 完成时间

    # 关联关系
    kg_task = relationship("KnowledgeGraphTask", overlaps="chapter_statuses")
    chapter = relationship("Chapter")

class KnowledgeGraphConfig(Base):
    """知识图谱解析配置"""
    __tablename__ = 'knowledge_graph_configs'
    __table_args__ = (
        Index('idx_kg_config_name', 'name'),
        Index('idx_kg_config_is_default', 'is_default'),
        Index('idx_kg_config_created_at', 'created_at'),
    )

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)  # 配置名称
    description = Column(Text)  # 配置描述
    
    # AI服务配置
    ai_provider = Column(String(50))  # AI服务商：openai, claude, deepseek, zhipu 等
    ai_model = Column(String(100))    # AI模型名称
    use_ai = Column(Boolean, default=True)  # 是否使用AI提取，False则仅使用规则提取
    
    # 提取配置
    max_content_length = Column(Integer, default=4000)  # 最大内容长度（防止超出token限制）
    enable_entity_extraction = Column(Boolean, default=True)    # 启用实体提取
    enable_relation_extraction = Column(Boolean, default=True)  # 启用关系提取
    
    # 实体类型配置（JSON格式）
    entity_types = Column(JSON, default=lambda: [
        {"type": "character", "name": "人物", "enabled": True},
        {"type": "location", "name": "地点", "enabled": True}, 
        {"type": "organization", "name": "组织", "enabled": True},
        {"type": "event", "name": "事件", "enabled": True}
    ])
    
    # 关系类型配置（JSON格式）
    relation_types = Column(JSON, default=lambda: [
        {"type": "FRIEND", "name": "朋友", "enabled": True},
        {"type": "ENEMY", "name": "敌人", "enabled": True},
        {"type": "LOVES", "name": "爱慕", "enabled": True},
        {"type": "HATES", "name": "仇恨", "enabled": True},
        {"type": "KNOWS", "name": "认识", "enabled": True},
        {"type": "LEADS", "name": "领导", "enabled": True},
        {"type": "FOLLOWS", "name": "跟随", "enabled": True},
        {"type": "PARTICIPATES_IN", "name": "参与", "enabled": True},
        {"type": "OCCURS_IN", "name": "发生于", "enabled": True}
    ])
    
    # 规则提取配置（JSON格式）
    rule_config = Column(JSON, default=lambda: {
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
    })
    
    # 系统标识
    is_default = Column(Boolean, default=False)  # 是否为默认配置
    is_system = Column(Boolean, default=False)   # 是否为系统配置（不可删除）
    
    # 时间戳
    created_at = Column(DateTime, default=beijing_now)
    updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)

class TaskChapterStatus(Base):
    __tablename__ = 'task_chapter_status'
    __table_args__ = (
        Index('idx_task_chapter_status_task', 'task_id', 'task_type'),
        Index('idx_task_chapter_status_chapter', 'chapter_id'),
        Index('idx_task_chapter_status_status', 'status'),
    )
    
    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, nullable=False)  # 任务ID（可以是批量分析任务或工作流执行任务）
    task_type = Column(String(50), nullable=False)  # 任务类型：'analysis' 或 'workflow'
    chapter_id = Column(Integer, ForeignKey('chapters.id'), nullable=False)
    status = Column(String(20), nullable=False)  # pending, running, completed, failed, stopped
    
    # 步骤信息（用于工作流）
    current_step = Column(String(100))  # 当前步骤名称
    step_order = Column(Integer)  # 当前步骤序号
    total_steps = Column(Integer)  # 总步骤数
    
    # 时间戳
    created_at = Column(DateTime, default=beijing_now)
    updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)
    
    # 关联关系
    chapter = relationship("Chapter")

class User(Base):
    __tablename__ = 'users'
    __table_args__ = (
        Index('idx_users_username', 'username'),
    )
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), nullable=False, unique=True)
    password_hash = Column(String(255), nullable=False)
    email = Column(String(100))
    role = Column(String(20), default='user')  # user, admin
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=beijing_now)
    updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)
    last_login = Column(DateTime)
    
    # 关联关系
    sessions = relationship("UserSession", back_populates="user", cascade="all, delete-orphan")
    operation_logs = relationship("OperationLog", back_populates="user", cascade="all, delete-orphan")

class UserSession(Base):
    __tablename__ = 'user_sessions'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    token_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=beijing_now)
    expires_at = Column(DateTime, nullable=False)
    is_active = Column(Boolean, default=True)
    
    # 关联关系
    user = relationship("User", back_populates="sessions")

class OperationLog(Base):
    __tablename__ = 'operation_logs'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    operation_type = Column(String(50), nullable=False)  # login, logout, create, update, delete
    target_type = Column(String(50))  # novel, chapter, analysis, workflow
    target_id = Column(String(100))
    description = Column(Text)
    ip_address = Column(String(45))
    user_agent = Column(Text)
    created_at = Column(DateTime, default=beijing_now)
    
    # 关联关系
    user = relationship("User", back_populates="operation_logs")

class ExportTask(Base):
    __tablename__ = 'export_tasks'
    __table_args__ = (
        Index('idx_export_tasks_task_id', 'task_id'),
        Index('idx_export_tasks_status', 'status'),
        Index('idx_export_tasks_created_at', 'created_at'),
        Index('idx_export_tasks_updated_at', 'updated_at'),
    )
    
    id = Column(Integer, primary_key=True)
    export_id = Column(String(255), nullable=False, unique=True)  # UUID
    task_id = Column(String(255), nullable=False)  # 关联的任务ID
    task_name = Column(String(255))  # 任务名称
    status = Column(String(50), nullable=False, default='pending')  # pending, running, completed, failed, cancelled
    progress = Column(Float, default=0.0)  # 进度百分比
    current_step = Column(Integer, default=0)
    total_steps = Column(Integer, default=0)
    config = Column(JSON)  # 导出配置信息
    result_files = Column(JSON, default=list)  # 结果文件列表
    error_message = Column(Text)
    log_entries = Column(JSON, default=list)  # 日志条目
    created_at = Column(DateTime, default=beijing_now)
    updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)
    start_time = Column(DateTime)  # 开始执行时间
    end_time = Column(DateTime)  # 结束时间

class DatabaseManager:
    def __init__(self, database_url=None):
        if database_url is None:
            # 尝试从配置文件加载数据库配置
            database_url = self._get_database_url_from_config()
        
        # 获取连接池配置
        pool_config = self._get_pool_config()
        
        # 配置数据库连接池
        engine_kwargs = {
            # 连接池配置
            'pool_size': pool_config.get('pool_size', 10),
            'max_overflow': pool_config.get('max_overflow', 20),
            'pool_timeout': pool_config.get('pool_timeout', 30),
            'pool_recycle': pool_config.get('pool_recycle', 3600),
            'pool_pre_ping': pool_config.get('pool_pre_ping', True),
            # 连接池日志
            'echo': False,
            # 其他配置
            'future': True
        }
        
        # SQLite特定配置
        if 'sqlite' in database_url:
            engine_kwargs['connect_args'] = {
                'check_same_thread': False,  # SQLite允许多线程
                'timeout': 20                # SQLite锁等待超时
            }
        # MySQL特定配置
        elif 'mysql' in database_url:
            engine_kwargs['connect_args'] = {
                'charset': 'utf8mb4',
                'autocommit': False,
                'init_command': 'SET sql_mode="STRICT_TRANS_TABLES"'
            }
        
        self.engine = create_engine(database_url, **engine_kwargs)

        # 为MySQL设置时区
        if 'mysql' in database_url:
            @event.listens_for(self.engine, "connect")
            def set_timezone(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute("SET time_zone = '+08:00'")
                cursor.close()

        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
            expire_on_commit=False  # 避免对象在commit后过期
        )
    
    @staticmethod
    def encode_connection_string(connection_string):
        """将数据库连接字符串进行base64编码"""
        return base64.b64encode(connection_string.encode('utf-8')).decode('utf-8')
    
    @staticmethod
    def decode_connection_string(encoded_string):
        """将base64编码的连接字符串解码"""
        return base64.b64decode(encoded_string.encode('utf-8')).decode('utf-8')
    
    def _get_pool_config(self):
        """获取连接池配置，优先从环境变量读取"""
        # 从环境变量读取连接池配置
        pool_config = {}

        # Worker 节点应使用更小的连接池（每个进程只需少量连接）
        # 默认值：pool_size=2, max_overflow=3，每个进程最多 5 个连接
        if os.environ.get('DB_POOL_SIZE'):
            pool_config['pool_size'] = int(os.environ.get('DB_POOL_SIZE'))
        else:
            pool_config['pool_size'] = 2  # Worker 节点默认值，主节点可以更大

        if os.environ.get('DB_POOL_MAX_OVERFLOW'):
            pool_config['max_overflow'] = int(os.environ.get('DB_POOL_MAX_OVERFLOW'))
        else:
            pool_config['max_overflow'] = 3  # Worker 节点默认值

        if os.environ.get('DB_POOL_TIMEOUT'):
            pool_config['pool_timeout'] = int(os.environ.get('DB_POOL_TIMEOUT'))
        else:
            pool_config['pool_timeout'] = 30

        if os.environ.get('DB_POOL_RECYCLE'):
            pool_config['pool_recycle'] = int(os.environ.get('DB_POOL_RECYCLE'))
        else:
            pool_config['pool_recycle'] = 3600

        if os.environ.get('DB_POOL_PRE_PING'):
            pool_config['pool_pre_ping'] = os.environ.get('DB_POOL_PRE_PING').lower() in ('true', '1', 'yes')
        else:
            pool_config['pool_pre_ping'] = True

        logger.info(f"[数据库] 连接池配置: pool_size={pool_config['pool_size']}, max_overflow={pool_config['max_overflow']}, 最大连接数={pool_config['pool_size'] + pool_config['max_overflow']}")

        # 如果环境变量未配置，尝试从 config.json 读取
        if not pool_config:
            try:
                from ..config import get_database_config
                return get_database_config().get('pool', {})
            except ImportError:
                # 后备方案
                try:
                    config_path = os.path.join(os.path.dirname(__file__), '../../config/config.json')
                    if os.path.exists(config_path):
                        with open(config_path, 'r', encoding='utf-8') as f:
                            config = json.load(f)
                        return config.get('database', {}).get('pool', {})
                except Exception:
                    pass
        return pool_config
    
    def _get_database_url_from_config(self):
        """从配置文件获取数据库URL，优先使用环境变量，默认使用SQLite，支持base64编码"""

        # 优先从环境变量读取数据库配置
        db_type_env = os.environ.get('DB_TYPE', '').lower()

        if db_type_env == 'mysql' or (os.environ.get('DB_HOST') and os.environ.get('DB_NAME')):
            # 从环境变量构建 MySQL 连接字符串
            try:
                host = os.environ.get('DB_HOST', 'localhost')
                port = int(os.environ.get('DB_PORT', '3306'))
                user = os.environ.get('DB_USER', 'root')
                password = os.environ.get('DB_PASSWORD', '')
                database = os.environ.get('DB_NAME', 'chaishu')
                charset = os.environ.get('DB_CHARSET', 'utf8mb4')

                from urllib.parse import quote_plus
                encoded_password = quote_plus(password)
                mysql_url = f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{database}?charset={charset}"
                logger.info(f"[数据库] 从环境变量加载 MySQL 配置: {host}:{port}/{database}")
                return mysql_url
            except Exception as e:
                logger.error(f"从环境变量构建 MySQL 连接失败: {e}")

        elif db_type_env == 'sqlite' or os.environ.get('DB_PATH'):
            # 从环境变量构建 SQLite 连接字符串
            db_path = os.environ.get('DB_PATH', 'data/chaishu.db')
            if not os.path.isabs(db_path):
                db_path = os.path.join(os.path.dirname(__file__), '../../', db_path)
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            logger.info(f"[数据库] 从环境变量加载 SQLite 配置: {db_path}")
            return f"sqlite:///{db_path}"

        # 如果环境变量未配置，尝试从 config.json 读取
        try:
            # 使用统一配置管理器
            from ..config import get_database_config
            db_config = get_database_config()
        except ImportError:
            # 后备方案：直接读取配置文件
            try:
                config_path = os.path.join(os.path.dirname(__file__), '../../config/config.json')
                if os.path.exists(config_path):
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                    db_config = config.get('database', {})
                else:
                    db_config = {}
            except Exception as e:
                logger.error(f"配置文件读取失败: {e}")
                db_config = {}

        try:
            if db_config:
                db_type = db_config.get('type', 'sqlite')
                
                # 检查是否使用base64编码的连接字符串
                if 'encoded_url' in db_config:
                    encoded_url = db_config['encoded_url']
                    return self.decode_connection_string(encoded_url)
                
                if db_type == 'mysql':
                    mysql_config = db_config.get('mysql', {})
                    host = mysql_config.get('host', 'localhost')
                    port = mysql_config.get('port', 3306)
                    user = mysql_config.get('user', 'root')
                    password = mysql_config.get('password', '')
                    database = mysql_config.get('database', 'chaishu')
                    charset = mysql_config.get('charset', 'utf8mb4')
                    
                    # 对所有base64编码的字段进行解码
                    try:
                        import base64
                        
                        def is_base64_and_decode(value):
                            """检测是否为base64编码并解码"""
                            if not isinstance(value, str) or len(value) < 4:
                                return value
                            # 检查是否以=结尾（base64 padding）或长度是4的倍数
                            if value.endswith('=') or (len(value) % 4 == 0 and value.replace('+', '').replace('/', '').replace('=', '').isalnum()):
                                try:
                                    decoded = base64.b64decode(value.encode('utf-8')).decode('utf-8')
                                    return decoded
                                except:
                                    return value
                            return value
                        
                        # 解码所有字段
                        host = is_base64_and_decode(host)
                        user = is_base64_and_decode(user)
                        password = is_base64_and_decode(password)
                        database = is_base64_and_decode(database)
                        
                        # 处理端口字段
                        if isinstance(port, str):
                            decoded_port = is_base64_and_decode(port)
                            if decoded_port.isdigit():
                                port = int(decoded_port)
                            elif port.isdigit():
                                port = int(port)
                        
                    except Exception:
                        pass  # 如果解码失败，使用原值
                    
                    # MySQL连接池配置 - 对密码进行URL编码处理特殊字符
                    from urllib.parse import quote_plus
                    encoded_password = quote_plus(password)
                    mysql_url = f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{database}?charset={charset}"
                    return mysql_url
                
                elif db_type == 'sqlite':
                    sqlite_config = db_config.get('sqlite', {})
                    db_path = sqlite_config.get('path', 'data/chaishu.db')
                    
                    # 确保路径是绝对路径
                    if not os.path.isabs(db_path):
                        db_path = os.path.join(os.path.dirname(__file__), '../../', db_path)
                    
                    # 确保数据目录存在
                    os.makedirs(os.path.dirname(db_path), exist_ok=True)
                    
                    return f"sqlite:///{db_path}"
        
        except Exception as e:
            print(f"警告: 读取数据库配置失败: {e}")
        
        # 默认配置：使用SQLite数据库
        default_db_path = os.path.join(os.path.dirname(__file__), '../../data/chaishu.db')
        os.makedirs(os.path.dirname(default_db_path), exist_ok=True)
        return f"sqlite:///{default_db_path}"
        
    def create_tables(self):
        """创建数据库表"""
        Base.metadata.create_all(bind=self.engine)
    
    def get_session(self):
        """获取数据库会话"""
        return self.SessionLocal()
    
    def get_connection_pool_status(self):
        """获取连接池状态"""
        pool = self.engine.pool
        return {
            'pool_size': pool.size(),
            'checked_in_connections': pool.checkedin(),
            'checked_out_connections': pool.checkedout(),
            'overflow_connections': pool.overflow(),
            'total_connections': pool.size() + pool.overflow()
        }
    
    def close_all_connections(self):
        """关闭所有连接"""
        self.engine.dispose()
    
    def test_connection(self):
        """测试数据库连接"""
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"数据库连接测试失败: {e}")
            return False
    
    def init_default_data(self):
        session = self.get_session()
        try:
            # 创建默认AI服务商
            if not session.query(AIProvider).filter_by(name='openai').first():
                openai_provider = AIProvider(
                    name='openai',
                    display_name='OpenAI',
                    base_url='https://api.openai.com/v1',
                    models=['gpt-4', 'gpt-4-turbo', 'gpt-3.5-turbo'],
                    is_active=True
                )
                session.add(openai_provider)
            
            if not session.query(AIProvider).filter_by(name='claude').first():
                claude_provider = AIProvider(
                    name='claude',
                    display_name='Claude',
                    base_url='https://api.anthropic.com/v1',
                    models=['claude-3-opus', 'claude-3-sonnet', 'claude-3-haiku'],
                    is_active=True
                )
                session.add(claude_provider)
            
            if not session.query(AIProvider).filter_by(name='zhipu').first():
                zhipu_provider = AIProvider(
                    name='zhipu',
                    display_name='智谱AI',
                    base_url='https://open.bigmodel.cn/api/paas/v4',
                    models=['glm-4', 'glm-4v', 'glm-3-turbo'],
                    is_active=True
                )
                session.add(zhipu_provider)
            
            if not session.query(AIProvider).filter_by(name='deepseek').first():
                deepseek_provider = AIProvider(
                    name='deepseek',
                    display_name='DeepSeek',
                    base_url='https://api.deepseek.com/v1',
                    models=['deepseek-chat', 'deepseek-coder'],
                    is_active=True
                )
                session.add(deepseek_provider)
            
            # 添加本地AI服务商
            if not session.query(AIProvider).filter_by(name='ollama').first():
                ollama_provider = AIProvider(
                    name='ollama',
                    display_name='Ollama (本地)',
                    base_url='http://localhost:11434',
                    models=['llama2', 'codellama', 'qwen', 'chatglm3'],  # 常见模型示例
                    is_active=False  # 默认不激活，需要用户配置
                )
                session.add(ollama_provider)
            
            if not session.query(AIProvider).filter_by(name='localai').first():
                localai_provider = AIProvider(
                    name='localai',
                    display_name='LocalAI (本地)',
                    base_url='http://localhost:8080',
                    models=['gpt-3.5-turbo', 'gpt-4'],  # 兼容模型名
                    is_active=False  # 默认不激活，需要用户配置
                )
                session.add(localai_provider)
            
            if not session.query(AIProvider).filter_by(name='openai-compatible').first():
                openai_compatible_provider = AIProvider(
                    name='openai-compatible',
                    display_name='OpenAI兼容服务 (本地)',
                    base_url='http://localhost:8000',
                    models=['gpt-3.5-turbo', 'gpt-4'],  # 兼容模型名
                    is_active=False  # 默认不激活，需要用户配置
                )
                session.add(openai_compatible_provider)
            
            # 创建默认分类的占位符模板（确保默认分类存在）
            if not session.query(PromptTemplate).filter_by(name='_category_placeholder_默认').first():
                default_category_placeholder = PromptTemplate(
                    name='_category_placeholder_默认',
                    category='默认',
                    description='系统默认分类占位符，请勿删除',
                    template='# 这是默认分类的占位符模板\n# 用于在系统中建立默认分类: {category_name}',
                    variables=['category_name'],
                    is_default=False,
                    is_system_category=True  # 标记为系统分类
                )
                session.add(default_category_placeholder)
            
            # 创建拆书流程分类的占位符模板
            if not session.query(PromptTemplate).filter_by(name='_category_placeholder_拆书流程').first():
                chaishu_category_placeholder = PromptTemplate(
                    name='_category_placeholder_拆书流程',
                    category='拆书流程',
                    description='拆书流程分类占位符，请勿删除',
                    template='# 这是拆书流程分类的占位符模板\n# 用于在系统中建立拆书流程分类: {category_name}',
                    variables=['category_name'],
                    is_default=False,
                    is_system_category=True  # 标记为系统分类
                )
                session.add(chaishu_category_placeholder)
            
            # 创建默认提示词模板（全部放在默认分类中）
            default_templates = [
                {
                    'name': '剧情摘要',
                    'category': '默认',
                    'description': '提取章节核心剧情和关键事件',
                    'template': '''请对以下小说章节进行剧情摘要分析：

章节标题：{title}
章节内容：{content}

请按以下格式输出：
1. 核心剧情（3-5句话概括主要情节）
2. 关键事件（列出2-3个重要事件）
3. 人物行动（主要人物的重要行为）
4. 情节转折（如果有的话）

请确保摘要准确且简洁。''',
                    'variables': ['title', 'content'],
                    'is_default': True
                },
                {
                    'name': '章节大纲',
                    'category': '默认',
                    'description': '分析章节结构和叙事节奏',
                    'template': '''请对以下小说章节进行结构分析：

章节标题：{title}
章节内容：{content}

请按以下格式输出：
1. 章节结构（开头、发展、高潮、结尾）
2. 叙事节奏（快/中/慢，及其原因）
3. 写作技巧（使用的主要技巧）
4. 章节功能（在整体故事中的作用）

请提供详细的分析。''',
                    'variables': ['title', 'content'],
                    'is_default': True
                },
                {
                    'name': '角色分析',
                    'category': '默认',
                    'description': '分析章节中的人物关系和性格发展',
                    'template': '''请对以下小说章节进行角色分析：

章节标题：{title}
章节内容：{content}

请按以下格式输出：
1. 主要角色（出现的重要人物）
2. 人物关系（角色之间的关系变化）
3. 性格展现（通过行为和对话展现的性格特点）
4. 角色发展（人物在本章的成长或变化）

请深入分析人物的心理和动机。''',
                    'variables': ['title', 'content'],
                    'is_default': True
                },
                {
                    'name': '主题分析',
                    'category': '默认',
                    'description': '分析章节的主题思想和文化内涵',
                    'template': '''请对以下小说章节进行主题分析：

章节标题：{title}
章节内容：{content}

请按以下格式输出：
1. 主要主题（体现的核心思想）
2. 价值观念（传达的价值观和世界观）
3. 文化背景（涉及的文化元素）
4. 深层含义（隐含的意义和象征）

请提供深入的主题解读。''',
                    'variables': ['title', 'content'],
                    'is_default': True
                },
                {
                    'name': '文风分析',
                    'category': '默认',
                    'description': '分析章节的语言风格和修辞手法',
                    'template': '''请对以下小说章节进行文风分析：

章节标题：{title}
章节内容：{content}

请按以下格式输出：
1. 语言风格（正式/通俗/诗意等）
2. 修辞手法（使用的主要修辞技巧）
3. 叙述技巧（叙述视角、时态等）
4. 文字特色（作者的语言特点）

请详细分析文学技法的运用。''',
                    'variables': ['title', 'content'],
                    'is_default': True
                },
                # 拆书流程专用模板
                {
                    'name': '提炼剧情',
                    'category': '拆书流程',
                    'description': '从小说正文中提炼出对应的剧情，用于后续的章节分析和大纲生成',
                    'template': '''**System:**
你需要参考一段小说的正文，提炼出对应的剧情。

在提炼剧情时，需要遵照以下原则：
1. 提炼的剧情和正文有一一对应，每行一句话，在50字以内，对应正文中一个关键场景或情节转折
2. 严格参照正文来提炼剧情，不能擅自延申、改编、删减，更不能在结尾进行总结、推演、展望
3. 不能有任何标题，序号，分点等
4. 对环境、心理、外貌、语言描写进行简化/概括
5. 在三引号(```)文本块中输出对应的剧情

**User:**
下面是一段正文，需要提炼出对应的剧情：
{chapter_content}''',
                    'variables': ['chapter_content'],
                    'is_default': False
                },
                {
                    'name': '提炼章节大纲',
                    'category': '拆书流程',
                    'description': '从章节剧情中提炼出章节大纲，关注事件脉络和关键转折点',
                    'template': '''**System:**
你需要参考一段小说的章节剧情，提炼出章节大纲。

在提炼章节大纲时，需要遵照以下原则：
1. 不能简单的总结，需要关注章节剧情中事件的脉络（起因、经过、高潮、结果），对事件进行提取并总结
2. 忽略不重要的细节（例如：环境、外貌、语言、心理描写）
3. 不能有任何标题，序号，分点等
4. 在三引号(```)文本块中输出对应的章节大纲

**User:**
下面是一段小说的章节剧情，需要提炼出章节大纲：
{chapter_plot}''',
                    'variables': ['chapter_plot'],
                    'is_default': False
                },
                {
                    'name': '提炼全书大纲',
                    'category': '拆书流程',
                    'description': '从所有章节内容中提炼出整本小说的大纲，关注整体故事脉络',
                    'template': '''**System:**
你需要参考小说的章节，提炼出小说大纲。

在提炼小说大纲时，需要遵照以下原则：
1. 关注整个小说的故事脉络，对故事进行提取并总结
2. 不要逐章总结，关注整体故事发展
3. 在三引号(```)文本块中输出小说大纲

**User:**
下面是小说章节，需要提炼出小说大纲：
{all_chapters}''',
                    'variables': ['all_chapters'],
                    'is_default': False
                }
            ]
            
            for template_data in default_templates:
                if not session.query(PromptTemplate).filter_by(name=template_data['name']).first():
                    template = PromptTemplate(**template_data)
                    session.add(template)
            
            # 创建默认工作流
            if not session.query(Workflow).filter_by(name='智能拆书专业流程').first():
                workflow = Workflow(
                    name='智能拆书专业流程',
                    description='基于Long-Novel-GPT-v3.0的三阶段智能拆书流程：剧情提炼 → 章节大纲 → 全书大纲',
                    category='analysis',
                    is_active=True,
                    is_default=True
                )
                session.add(workflow)
                session.flush()  # 获取workflow的id
                
                # 创建工作流步骤
                default_steps = [
                    {
                        'name': '剧情提炼',
                        'description': '从原始章节内容中提炼关键剧情要点，每行对应一个关键场景',
                        'template_name': '提炼剧情',
                        'input_source': 'original',
                        'order_index': 1,
                        'is_optional': False,
                        'scope': 'chapter',
                        'provider_name': 'deepseek',  # 使用通用名称，不包含 free 后缀
                        'model_name': 'deepseek-chat'
                    },
                    {
                        'name': '章节大纲生成',
                        'description': '基于提炼的剧情生成章节大纲，关注事件脉络和转折点',
                        'template_name': '提炼章节大纲',
                        'input_source': 'previous',
                        'order_index': 2,
                        'is_optional': False,
                        'scope': 'chapter',
                        'provider_name': 'deepseek',
                        'model_name': 'deepseek-chat'
                    },
                    {
                        'name': '全书大纲提炼',
                        'description': '基于所有章节大纲提炼全书大纲，关注整体故事脉络',
                        'template_name': '提炼全书大纲',
                        'input_source': 'custom',
                        'order_index': 3,
                        'is_optional': True,
                        'scope': 'global',
                        'provider_name': 'deepseek',
                        'model_name': 'deepseek-chat'
                    }
                ]
                
                for step_data in default_steps:
                    # 查找对应的 prompt_template_id
                    template_name = step_data['template_name']
                    template = session.query(PromptTemplate).filter_by(name=template_name).first()
                    if template:
                        step_data['prompt_template_id'] = template.id
                    
                    step = WorkflowStep(
                        workflow_id=workflow.id,
                        **step_data
                    )
                    session.add(step)
            
            # 创建默认知识图谱配置
            if not session.query(KnowledgeGraphConfig).filter_by(is_default=True).first():
                # 查找第一个可用的AI服务商
                available_provider = session.query(AIProvider).filter(
                    AIProvider.api_key.isnot(None),
                    AIProvider.api_key != ''
                ).first()
                
                # 如果没有配置API密钥的服务商，使用openai-compatible作为占位符
                if not available_provider:
                    available_provider = session.query(AIProvider).filter_by(
                        name='openai-compatible'
                    ).first()
                
                default_config = KnowledgeGraphConfig(
                    name='默认配置',
                    description='系统默认的知识图谱解析配置，使用规则提取作为备选',
                    ai_provider=available_provider.name if available_provider else None,
                    ai_model=available_provider.models[0] if available_provider and available_provider.models else None,
                    use_ai=bool(available_provider and available_provider.api_key),
                    max_content_length=4000,
                    enable_entity_extraction=True,
                    enable_relation_extraction=True,
                    is_default=True,
                    is_system=True
                )
                session.add(default_config)
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

# 全局数据库管理器实例
db_manager = DatabaseManager()