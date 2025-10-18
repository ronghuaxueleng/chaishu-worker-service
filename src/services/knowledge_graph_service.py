"""
知识图谱服务模块
用于管理小说人物、事件的知识图谱
"""
import logging
import os
import time
from functools import wraps
from typing import Dict, List, Any, Optional, Tuple
from neo4j import GraphDatabase, exceptions
from neo4j.exceptions import ServiceUnavailable

logger = logging.getLogger(__name__)


def retry_on_defunct_connection(max_retries=2, delay=0.5):
    """装饰器：当遇到defunct连接时自动重试"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return func(self, *args, **kwargs)
                except ServiceUnavailable as e:
                    last_error = e
                    if "defunct" in str(e).lower() and attempt < max_retries - 1:
                        logger.warning(f"检测到defunct连接，第{attempt + 1}次重试 {func.__name__}")
                        # 强制重新连接
                        try:
                            if self.driver:
                                self.driver.close()
                        except:
                            pass
                        self.driver = None
                        self._connect()
                        time.sleep(delay)
                    else:
                        raise
            raise last_error
        return wrapper
    return decorator


class KnowledgeGraphService:
    """知识图谱服务类"""

    def __init__(self):
        """初始化知识图谱服务"""
        self.driver = None
        self._connect()

    def _connect(self):
        """连接到Neo4j数据库（带连接池和重试机制）"""
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                # 从环境变量读取 Neo4j 配置
                uri = os.environ.get('NEO4J_URI', 'neo4j://localhost:7687')
                user = os.environ.get('NEO4J_USER', 'neo4j')
                password = os.environ.get('NEO4J_PASSWORD', 'password')

                # 配置连接池参数（优化网络稳定性）
                config_options = {
                    'max_connection_pool_size': 10,  # 减小连接池大小
                    'max_connection_lifetime': 60,  # 连接最大生存时间1分钟（避免defunct连接）
                    'connection_acquisition_timeout': 10,  # 获取连接超时（秒）
                    'connection_timeout': 5,  # 连接超时（秒）
                    'keep_alive': True,  # 保持连接活跃
                }

                self.driver = GraphDatabase.driver(uri, auth=(user, password), **config_options)
                self.driver.verify_connectivity()
                logger.info(f"成功连接到Neo4j数据库: {uri} (连接池大小: {config_options['max_connection_pool_size']})")

                # 初始化约束和索引
                self._initialize_constraints_and_indexes()
                return

            except Exception as e:
                logger.warning(f"第{attempt + 1}次连接Neo4j失败: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"{retry_delay}秒后重试...")
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    logger.error(f"连接Neo4j数据库失败，已重试{max_retries}次")
                    raise

    def _initialize_constraints_and_indexes(self):
        """初始化约束和索引"""
        constraints_and_indexes = [
            # 约束：确保节点的唯一性
            "CREATE CONSTRAINT character_name_novel_unique IF NOT EXISTS FOR (c:Character) REQUIRE (c.name, c.novel_id) IS UNIQUE",
            "CREATE CONSTRAINT event_id_unique IF NOT EXISTS FOR (e:Event) REQUIRE e.id IS UNIQUE",
            "CREATE CONSTRAINT location_name_novel_unique IF NOT EXISTS FOR (l:Location) REQUIRE (l.name, l.novel_id) IS UNIQUE",
            "CREATE CONSTRAINT organization_name_novel_unique IF NOT EXISTS FOR (o:Organization) REQUIRE (o.name, o.novel_id) IS UNIQUE",
            "CREATE CONSTRAINT chapter_id_unique IF NOT EXISTS FOR (ch:Chapter) REQUIRE ch.id IS UNIQUE",
            "CREATE CONSTRAINT novel_id_unique IF NOT EXISTS FOR (n:Novel) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT plot_id_unique IF NOT EXISTS FOR (p:Plot) REQUIRE p.id IS UNIQUE",

            # 索引：提高查询性能
            "CREATE INDEX character_name_index IF NOT EXISTS FOR (c:Character) ON (c.name)",
            "CREATE INDEX character_novel_index IF NOT EXISTS FOR (c:Character) ON (c.novel_id)",
            "CREATE INDEX event_chapter_index IF NOT EXISTS FOR (e:Event) ON (e.chapter_id)",
            "CREATE INDEX event_novel_index IF NOT EXISTS FOR (e:Event) ON (e.novel_id)",
            "CREATE INDEX location_name_index IF NOT EXISTS FOR (l:Location) ON (l.name)",
            "CREATE INDEX location_novel_index IF NOT EXISTS FOR (l:Location) ON (l.novel_id)",
            "CREATE INDEX organization_name_index IF NOT EXISTS FOR (o:Organization) ON (o.name)",
            "CREATE INDEX organization_novel_index IF NOT EXISTS FOR (o:Organization) ON (o.novel_id)",
            "CREATE INDEX plot_novel_index IF NOT EXISTS FOR (p:Plot) ON (p.novel_id)",
            "CREATE INDEX plot_type_index IF NOT EXISTS FOR (p:Plot) ON (p.type)",
        ]

        with self.driver.session() as session:
            for query in constraints_and_indexes:
                try:
                    session.run(query)
                except Exception as e:
                    logger.warning(f"执行约束/索引创建失败: {query}, 错误: {e}")

    def close(self):
        """关闭数据库连接"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j连接已关闭")

    def _ensure_connection(self):
        """确保连接可用，必要时重新连接"""
        try:
            if self.driver:
                self.driver.verify_connectivity()
            return True
        except Exception as e:
            logger.warning(f"Neo4j连接检查失败，尝试重新连接: {e}")
            try:
                # 先关闭旧连接
                if self.driver:
                    try:
                        self.driver.close()
                    except:
                        pass
                    self.driver = None
                # 重新连接
                self._connect()
                return True
            except Exception as reconnect_e:
                logger.error(f"Neo4j重新连接失败: {reconnect_e}")
                raise RuntimeError(f"Neo4j连接不可用: {reconnect_e}")

    def _execute_with_retry(self, operation_func, *args, **kwargs):
        """带重试机制的操作执行"""
        max_retries = 2

        for attempt in range(max_retries + 1):
            try:
                if not self._ensure_connection():
                    raise Exception("无法建立Neo4j连接")

                return operation_func(*args, **kwargs)

            except Exception as e:
                if attempt < max_retries:
                    logger.error(f"Neo4j操作失败，第{attempt + 1}次重试: {e}")
                    import time
                    time.sleep(1)
                else:
                    logger.error(f"Neo4j操作最终失败（已重试{max_retries}次）: {e}")
                    raise

    # === 节点创建方法 ===

    def create_character(self, name: str, novel_id: int, task_id: int = None, **properties) -> Dict:
        """创建人物节点"""
        def _create_character_operation():
            query = """
            MERGE (c:Character {name: $name, novel_id: $novel_id})
            SET c += $properties
            SET c.created_at = datetime()
            SET c.updated_at = datetime()
            """
            
            # 如果提供了task_id，将其添加到节点属性中
            if task_id is not None:
                query += """
                SET c.task_id = CASE WHEN c.task_id IS NULL THEN [$task_id] 
                                    WHEN NOT $task_id IN c.task_id THEN c.task_id + [$task_id]
                                    ELSE c.task_id END
                """
            
            query += " RETURN c"

            with self.driver.session() as session:
                result = session.run(query, name=name, novel_id=novel_id, task_id=task_id, properties=properties)
                record = result.single()
                if record:
                    return dict(record['c'])
                return {}

        return self._execute_with_retry(_create_character_operation)

    def create_event(self, event_id: str, name: str, chapter_id: int, novel_id: int, task_id: int = None, **properties) -> Dict:
        """创建事件节点"""
        def _create_event_operation():
            from ..models.database import db_manager
            from sqlalchemy import text as sql_text

            # 先从数据库查询章节号
            chapter_number = None
            db_session = db_manager.get_session()
            try:
                result = db_session.execute(
                    sql_text("SELECT chapter_number FROM chapters WHERE id = :chapter_id"),
                    {"chapter_id": chapter_id}
                ).fetchone()
                if result:
                    chapter_number = result[0]
            except Exception as e:
                logger.warning(f"查询章节号失败 (chapter_id={chapter_id}): {e}")
            finally:
                db_session.close()

            query = """
            MERGE (e:Event {id: $event_id})
            SET e.name = $name
            SET e.chapter_id = $chapter_id
            SET e.chapter_number = $chapter_number
            SET e.novel_id = $novel_id
            SET e += $properties
            SET e.created_at = datetime()
            SET e.updated_at = datetime()
            """

            # 如果提供了task_id，将其添加到节点属性中
            if task_id is not None:
                query += """
                SET e.task_id = CASE WHEN e.task_id IS NULL THEN [$task_id]
                                    WHEN NOT $task_id IN e.task_id THEN e.task_id + [$task_id]
                                    ELSE e.task_id END
                """

            query += " RETURN e"

            with self.driver.session() as session:
                result = session.run(query,
                                   event_id=event_id,
                                   name=name,
                                   chapter_id=chapter_id,
                                   chapter_number=chapter_number,
                                   novel_id=novel_id,
                                   task_id=task_id,
                                   properties=properties)
                record = result.single()
                if record:
                    return dict(record['e'])
                return {}

        return self._execute_with_retry(_create_event_operation)

    def create_location(self, name: str, novel_id: int, task_id: int = None, **properties) -> Dict:
        """创建地点节点"""
        def _create_location_operation():
            query = """
            MERGE (l:Location {name: $name, novel_id: $novel_id})
            SET l += $properties
            SET l.created_at = datetime()
            SET l.updated_at = datetime()
            """

            # 如果提供了task_id，将其添加到节点属性中
            if task_id is not None:
                query += """
                SET l.task_id = CASE WHEN l.task_id IS NULL THEN [$task_id]
                                    WHEN NOT $task_id IN l.task_id THEN l.task_id + [$task_id]
                                    ELSE l.task_id END
                """

            query += " RETURN l"

            with self.driver.session() as session:
                result = session.run(query, name=name, novel_id=novel_id, task_id=task_id, properties=properties)
                record = result.single()
                if record:
                    return dict(record['l'])
                return {}

        return self._execute_with_retry(_create_location_operation)

    def create_organization(self, name: str, novel_id: int, task_id: int = None, **properties) -> Dict:
        """创建组织节点"""
        def _create_organization_operation():
            query = """
            MERGE (o:Organization {name: $name, novel_id: $novel_id})
            SET o += $properties
            SET o.created_at = datetime()
            SET o.updated_at = datetime()
            """

            # 如果提供了task_id，将其添加到节点属性中
            if task_id is not None:
                query += """
                SET o.task_id = CASE WHEN o.task_id IS NULL THEN [$task_id]
                                    WHEN NOT $task_id IN o.task_id THEN o.task_id + [$task_id]
                                    ELSE o.task_id END
                """

            query += " RETURN o"

            with self.driver.session() as session:
                result = session.run(query, name=name, novel_id=novel_id, task_id=task_id, properties=properties)
                record = result.single()
                if record:
                    return dict(record['o'])
                return {}

        return self._execute_with_retry(_create_organization_operation)

    def create_chapter_node(self, chapter_id: int, title: str, novel_id: int, task_id: int = None, **properties) -> Dict:
        """创建章节节点"""
        def _create_chapter_operation():
            query = """
            MERGE (ch:Chapter {id: $chapter_id})
            SET ch.title = $title
            SET ch.novel_id = $novel_id
            SET ch += $properties
            SET ch.created_at = datetime()
            SET ch.updated_at = datetime()
            """

            # 如果提供了task_id，将其添加到节点属性中
            if task_id is not None:
                query += """
                SET ch.task_id = CASE WHEN ch.task_id IS NULL THEN [$task_id]
                                    WHEN NOT $task_id IN ch.task_id THEN ch.task_id + [$task_id]
                                    ELSE ch.task_id END
                """

            query += " RETURN ch"

            with self.driver.session() as session:
                result = session.run(query,
                                   chapter_id=chapter_id,
                                   title=title,
                                   novel_id=novel_id,
                                   task_id=task_id,
                                   properties=properties)
                record = result.single()
                if record:
                    return dict(record['ch'])
                return {}

        return self._execute_with_retry(_create_chapter_operation)

    def create_novel_node(self, novel_id: int, title: str, author: str = None, **properties) -> Dict:
        """创建小说节点"""
        def _create_novel_operation():
            query = """
            MERGE (n:Novel {id: $novel_id})
            SET n.title = $title
            SET n.author = $author
            SET n += $properties
            SET n.created_at = datetime()
            SET n.updated_at = datetime()
            RETURN n
            """

            with self.driver.session() as session:
                result = session.run(query,
                                   novel_id=novel_id,
                                   title=title,
                                   author=author,
                                   properties=properties)
                record = result.single()
                if record:
                    return dict(record['n'])
                return {}

        return self._execute_with_retry(_create_novel_operation)

    # === 关系创建方法 ===

    def create_relationship(self, from_label: str, from_property: str, from_value: Any,
                          to_label: str, to_property: str, to_value: Any,
                          relationship_type: str, **properties) -> bool:
        """创建关系"""
        def _create_relationship_operation():
            query = f"""
            MATCH (a:{from_label} {{{from_property}: $from_value}})
            MATCH (b:{to_label} {{{to_property}: $to_value}})
            MERGE (a)-[r:{relationship_type}]->(b)
            SET r += $properties
            SET r.created_at = datetime()
            RETURN r
            """

            with self.driver.session() as session:
                result = session.run(query,
                                   from_value=from_value,
                                   to_value=to_value,
                                   properties=properties)
                return result.single() is not None

        return self._execute_with_retry(_create_relationship_operation)

    def character_appears_in_chapter(self, character_name: str, chapter_id: int, novel_id: int, **properties) -> bool:
        """人物出现在章节"""
        return self.create_relationship(
            "Character", "name", character_name,
            "Chapter", "id", chapter_id,
            "APPEARS_IN",
            novel_id=novel_id,
            **properties
        )

    def character_participates_in_event(self, character_name: str, event_id: str, novel_id: int, role: str = None, **properties) -> bool:
        """人物参与事件"""
        props = {"novel_id": novel_id}
        if role:
            props["role"] = role
        props.update(properties)

        return self.create_relationship(
            "Character", "name", character_name,
            "Event", "id", event_id,
            "PARTICIPATES_IN",
            **props
        )

    def event_occurs_in_location(self, event_id: str, location_name: str, novel_id: int, **properties) -> bool:
        """事件发生在地点"""
        return self.create_relationship(
            "Event", "id", event_id,
            "Location", "name", location_name,
            "OCCURS_IN",
            novel_id=novel_id,
            **properties
        )

    def character_belongs_to_organization(self, character_name: str, organization_name: str, novel_id: int, position: str = None, **properties) -> bool:
        """人物属于组织"""
        props = {"novel_id": novel_id}
        if position:
            props["position"] = position
        props.update(properties)

        return self.create_relationship(
            "Character", "name", character_name,
            "Organization", "name", organization_name,
            "BELONGS_TO",
            **props
        )

    def character_relationship(self, from_character: str, to_character: str, relationship_type: str, novel_id: int, **properties) -> bool:
        """人物关系（KNOWS, FRIEND, ENEMY, LOVES, HATES等）"""
        return self.create_relationship(
            "Character", "name", from_character,
            "Character", "name", to_character,
            relationship_type,
            novel_id=novel_id,
            **properties
        )

    # === 查询方法 ===

    def get_character_by_name(self, name: str, novel_id: int = None) -> Optional[Dict]:
        """根据名称查询人物"""
        if novel_id:
            query = "MATCH (c:Character {name: $name, novel_id: $novel_id}) RETURN c"
            params = {"name": name, "novel_id": novel_id}
        else:
            query = "MATCH (c:Character {name: $name}) RETURN c"
            params = {"name": name}

        with self.driver.session() as session:
            result = session.run(query, **params)
            record = result.single()
            if record:
                return dict(record['c'])
            return None

    def get_characters_by_novel(self, novel_id: int) -> List[Dict]:
        """获取小说中的所有人物"""
        query = "MATCH (c:Character {novel_id: $novel_id}) RETURN c ORDER BY c.name"

        with self.driver.session() as session:
            result = session.run(query, novel_id=novel_id)
            return [dict(record['c']) for record in result]

    def get_events_by_chapter(self, chapter_id: int) -> List[Dict]:
        """获取章节中的所有事件"""
        query = "MATCH (e:Event {chapter_id: $chapter_id}) RETURN e ORDER BY e.name"

        with self.driver.session() as session:
            result = session.run(query, chapter_id=chapter_id)
            return [dict(record['e']) for record in result]

    def get_character_relationships(self, character_name: str, novel_id: int = None) -> List[Dict]:
        """获取人物的所有关系"""
        if novel_id:
            query = """
            MATCH (c:Character {name: $name, novel_id: $novel_id})-[r]-(other)
            RETURN type(r) as relationship_type, r as relationship, other, labels(other) as other_labels
            """
            params = {"name": character_name, "novel_id": novel_id}
        else:
            query = """
            MATCH (c:Character {name: $name})-[r]-(other)
            RETURN type(r) as relationship_type, r as relationship, other, labels(other) as other_labels
            """
            params = {"name": character_name}

        with self.driver.session() as session:
            result = session.run(query, **params)
            relationships = []
            for record in result:
                relationships.append({
                    'relationship_type': record['relationship_type'],
                    'relationship_properties': dict(record['relationship']),
                    'other_node': dict(record['other']),
                    'other_labels': record['other_labels']
                })
            return relationships

    def search_knowledge_graph(self, query_text: str, novel_id: int = None, limit: int = 20) -> List[Dict]:
        """搜索知识图谱"""
        search_conditions = []
        params = {"query_text": f".*{query_text}.*"}

        if novel_id:
            search_conditions.append("n.novel_id = $novel_id")
            params["novel_id"] = novel_id

        where_clause = f"WHERE {' AND '.join(search_conditions)}" if search_conditions else ""

        query = f"""
        MATCH (n)
        {where_clause}
        WHERE (n.name =~ $query_text OR n.title =~ $query_text OR n.description =~ $query_text)
        RETURN n, labels(n) as node_labels
        LIMIT $limit
        """
        params["limit"] = limit

        with self.driver.session() as session:
            result = session.run(query, **params)
            nodes = []
            for record in result:
                nodes.append({
                    'node': dict(record['n']),
                    'labels': record['node_labels']
                })
            return nodes

    def get_novel_knowledge_graph_stats(self, novel_id: int) -> Dict:
        """获取小说知识图谱统计信息"""
        query = """
        MATCH (n {novel_id: $novel_id})
        WITH labels(n) as node_labels, count(n) as count
        RETURN node_labels[0] as label, count
        UNION ALL
        MATCH (a {novel_id: $novel_id})-[r]-(b {novel_id: $novel_id})
        RETURN type(r) as label, count(r) as count
        """

        with self.driver.session() as session:
            result = session.run(query, novel_id=novel_id)
            stats = {}
            for record in result:
                stats[record['label']] = record['count']
            return stats

    def delete_novel_knowledge_graph(self, novel_id: int) -> bool:
        """删除小说的知识图谱"""
        query = """
        MATCH (n {novel_id: $novel_id})
        DETACH DELETE n
        """

        with self.driver.session() as session:
            result = session.run(query, novel_id=novel_id)
            return True

    def delete_task_knowledge_graph(self, novel_id: int, chapter_ids: List[int]) -> bool:
        """删除特定任务的知识图谱数据（只删除事件和章节，保留可复用的人物、地点等）"""
        try:
            with self.driver.session() as session:
                # 1. 删除特定章节的事件节点及其关系
                if chapter_ids:
                    event_query = """
                    MATCH (e:Event)
                    WHERE e.novel_id = $novel_id AND e.chapter_id IN $chapter_ids
                    DETACH DELETE e
                    """
                    session.run(event_query, novel_id=novel_id, chapter_ids=chapter_ids)
                    logger.info(f"删除了小说{novel_id}中章节{chapter_ids}的事件节点")

                    # 2. 删除特定章节节点及其关系
                    chapter_query = """
                    MATCH (ch:Chapter)
                    WHERE ch.id IN $chapter_ids
                    DETACH DELETE ch
                    """
                    session.run(chapter_query, chapter_ids=chapter_ids)
                    logger.info(f"删除了章节节点: {chapter_ids}")

                # 注意：我们不删除Character、Location、Organization节点，
                # 因为它们可能被其他任务共享使用
                
                logger.info(f"成功删除任务相关的知识图谱数据: 小说{novel_id}, 章节{chapter_ids}")
                return True
                
        except Exception as e:
            logger.error(f"删除任务知识图谱数据失败: {e}")
            return False

    def delete_task_nodes_by_task_id(self, task_id: int) -> bool:
        """根据task_id精确删除任务创建的节点"""
        try:
            with self.driver.session() as session:
                # 删除具有指定task_id的所有节点
                query = """
                MATCH (n)
                WHERE $task_id IN n.task_id
                WITH n, n.task_id as task_ids
                SET n.task_id = [tid IN task_ids WHERE tid <> $task_id]
                WITH n
                WHERE size(n.task_id) = 0
                DETACH DELETE n
                RETURN count(n) as deleted_count
                """
                
                result = session.run(query, task_id=task_id)
                record = result.single()
                deleted_count = record['deleted_count'] if record else 0
                
                logger.info(f"根据task_id={task_id}删除了{deleted_count}个节点")
                return True
                
        except Exception as e:
            logger.error(f"根据task_id删除节点失败: {e}")
            return False

    def test_connection(self) -> bool:
        """测试连接"""
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 as test")
                return result.single()['test'] == 1
        except Exception as e:
            logger.error(f"Neo4j连接测试失败: {e}")
            return False

    # ==================== Plot(情节)相关方法 ====================

    def create_plot(self, plot_id: str, name: str, novel_id: int, task_id: int = None, **properties) -> Dict:
        """
        创建情节节点

        Args:
            plot_id: 情节唯一标识
            name: 情节名称
            novel_id: 小说ID
            task_id: 任务ID
            **properties: 其他属性(type, start_chapter, end_chapter, core_conflict, summary等)
        """
        def _create_plot_operation():
            query = """
            MERGE (p:Plot {id: $plot_id})
            SET p.name = $name
            SET p.novel_id = $novel_id
            SET p += $properties
            SET p.created_at = datetime()
            SET p.updated_at = datetime()
            """

            if task_id is not None:
                query += """
                SET p.task_id = CASE WHEN p.task_id IS NULL THEN [$task_id]
                                    WHEN NOT $task_id IN p.task_id THEN p.task_id + [$task_id]
                                    ELSE p.task_id END
                """

            query += " RETURN p"

            with self.driver.session() as session:
                result = session.run(query,
                                   plot_id=plot_id,
                                   name=name,
                                   novel_id=novel_id,
                                   task_id=task_id,
                                   properties=properties)
                record = result.single()
                if record:
                    return dict(record['p'])
                return {}

        return self._execute_with_retry(_create_plot_operation)

    def update_plot(self, plot_id: str, **properties) -> Dict:
        """
        更新情节节点属性

        Args:
            plot_id: 情节唯一标识
            **properties: 要更新的属性(end_chapter, summary, themes, emotional_arc, status, key_turning_points等)
        """
        def _update_plot_operation():
            query = """
            MATCH (p:Plot {id: $plot_id})
            SET p += $properties
            SET p.updated_at = datetime()
            RETURN p
            """

            with self.driver.session() as session:
                result = session.run(query,
                                   plot_id=plot_id,
                                   properties=properties)
                record = result.single()
                if record:
                    return dict(record['p'])
                return {}

        return self._execute_with_retry(_update_plot_operation)

    def link_event_to_plot(self, event_id: str, plot_id: str, importance: str = 'important', sequence: int = 0) -> bool:
        """
        将事件关联到情节

        Args:
            event_id: 事件ID
            plot_id: 情节ID
            importance: 重要性(critical/important/minor)
            sequence: 在情节中的顺序
        """
        def _link_operation():
            query = """
            MATCH (e:Event {id: $event_id})
            MATCH (p:Plot {id: $plot_id})
            MERGE (e)-[r:PART_OF]->(p)
            SET r.importance = $importance
            SET r.sequence = $sequence
            RETURN r
            """

            with self.driver.session() as session:
                result = session.run(query,
                                   event_id=event_id,
                                   plot_id=plot_id,
                                   importance=importance,
                                   sequence=sequence)
                return result.single() is not None

        return self._execute_with_retry(_link_operation)

    def link_plot_to_chapter(self, plot_id: str, chapter_id: int, is_main_chapter: bool = False) -> bool:
        """
        将情节关联到章节

        Args:
            plot_id: 情节ID
            chapter_id: 章节ID
            is_main_chapter: 是否是主要章节
        """
        def _link_operation():
            query = """
            MATCH (p:Plot {id: $plot_id})
            MATCH (c:Chapter {id: $chapter_id})
            MERGE (p)-[r:HAPPENS_IN]->(c)
            SET r.is_main_chapter = $is_main_chapter
            RETURN r
            """

            with self.driver.session() as session:
                result = session.run(query,
                                   plot_id=plot_id,
                                   chapter_id=chapter_id,
                                   is_main_chapter=is_main_chapter)
                return result.single() is not None

        return self._execute_with_retry(_link_operation)

    def link_character_to_plot(self, character_name: str, plot_id: str, novel_id: int,
                               role: str = 'participant', arc: str = None) -> bool:
        """
        将角色关联到情节

        Args:
            character_name: 角色名
            plot_id: 情节ID
            novel_id: 小说ID
            role: 角色(protagonist/antagonist/supporter/witness)
            arc: 角色弧线描述
        """
        def _link_operation():
            query = """
            MATCH (c:Character {name: $character_name, novel_id: $novel_id})
            MATCH (p:Plot {id: $plot_id})
            MERGE (c)-[r:PARTICIPATES_IN]->(p)
            SET r.role = $role
            """
            if arc:
                query += " SET r.arc = $arc"
            query += " RETURN r"

            with self.driver.session() as session:
                result = session.run(query,
                                   character_name=character_name,
                                   plot_id=plot_id,
                                   novel_id=novel_id,
                                   role=role,
                                   arc=arc)
                return result.single() is not None

        return self._execute_with_retry(_link_operation)

    def create_plot_relationship(self, plot_a_id: str, plot_b_id: str,
                                 relationship_type: str, **properties) -> bool:
        """
        创建情节之间的关系

        Args:
            plot_a_id: 情节A的ID
            plot_b_id: 情节B的ID
            relationship_type: 关系类型(PRECEDES/PARALLEL_TO/CONFLICTS_WITH/INCLUDES/COMPLEMENTS)
            **properties: 关系属性
        """
        def _create_relationship_operation():
            query = f"""
            MATCH (pa:Plot {{id: $plot_a_id}})
            MATCH (pb:Plot {{id: $plot_b_id}})
            MERGE (pa)-[r:{relationship_type}]->(pb)
            SET r += $properties
            RETURN r
            """

            with self.driver.session() as session:
                result = session.run(query,
                                   plot_a_id=plot_a_id,
                                   plot_b_id=plot_b_id,
                                   properties=properties)
                return result.single() is not None

        return self._execute_with_retry(_create_relationship_operation)

    @retry_on_defunct_connection(max_retries=2, delay=0.5)
    def get_plots_by_task(self, task_id: int, page: int = 1, page_size: int = 20, search: str = '') -> Dict:
        """
        获取任务的情节（支持分页和搜索）

        Args:
            task_id: 任务ID
            page: 页码（从1开始）
            page_size: 每页大小
            search: 搜索关键词

        Returns:
            {
                'plots': [...],
                'total': 总数
            }
        """
        # 确保连接可用
        self._ensure_connection()

        # 构建搜索条件
        search_clause = ""
        params = {"task_id": task_id}

        if search and search.strip():
            search_clause = """
            AND (
                toLower(p.name) CONTAINS toLower($search)
                OR toLower(p.summary) CONTAINS toLower($search)
                OR toLower(p.core_conflict) CONTAINS toLower($search)
                OR ANY(char IN characters WHERE toLower(char) CONTAINS toLower($search))
            )
            """
            params["search"] = search.strip()

        # 1. 先获取总数
        count_query = f"""
        MATCH (p:Plot)
        WHERE $task_id IN p.task_id
        OPTIONAL MATCH (c:Character)-[:PARTICIPATES_IN]->(p)
        WITH p, collect(c.name) as characters
        WHERE 1=1 {search_clause}
        RETURN count(p) as total
        """

        # 2. 获取分页数据
        skip = (page - 1) * page_size
        params["skip"] = skip
        params["limit"] = page_size

        data_query = f"""
        MATCH (p:Plot)
        WHERE $task_id IN p.task_id
        OPTIONAL MATCH (c:Character)-[:PARTICIPATES_IN]->(p)
        WITH p, collect(c.name) as characters
        WHERE 1=1 {search_clause}
        RETURN p, characters
        ORDER BY p.start_chapter
        SKIP $skip
        LIMIT $limit
        """

        with self.driver.session() as session:
            # 获取总数
            count_result = session.run(count_query, params)
            total = count_result.single()['total']

            # 获取数据
            result = session.run(data_query, params)
            plots = []
            for record in result:
                plot = dict(record['p'])
                # 添加角色列表
                plot['characters'] = record['characters'] if record['characters'] else []
                plots.append(plot)

            return {
                'plots': plots,
                'total': total
            }


    def delete_plots_by_extraction_task(self, extraction_task_id: int, kg_task_id: int = None) -> int:
        """
        删除情节提取任务关联的所有情节数据

        Args:
            extraction_task_id: 情节提取任务ID
            kg_task_id: 知识图谱任务ID (用于删除旧版本数据和延续情节)

        Returns:
            删除的情节数量
        """
        # 确保连接可用
        self._ensure_connection()

        # 查询所有关联的情节（包括有extraction_task_id的和旧版本/延续情节通过task_id关联的）
        if kg_task_id:
            query_plots = """
            MATCH (p:Plot)
            WHERE p.extraction_task_id = $extraction_task_id
               OR ($kg_task_id IN p.task_id)
            RETURN p.id as plot_id
            """
            params = {"extraction_task_id": extraction_task_id, "kg_task_id": kg_task_id}
        else:
            query_plots = """
            MATCH (p:Plot)
            WHERE p.extraction_task_id = $extraction_task_id
            RETURN p.id as plot_id
            """
            params = {"extraction_task_id": extraction_task_id}

        with self.driver.session() as session:
            result = session.run(query_plots, **params)
            plot_ids = [record['plot_id'] for record in result]

            if not plot_ids:
                logger.info(f"未找到提取任务 {extraction_task_id} 关联的情节")
                return 0

            logger.info(f"找到 {len(plot_ids)} 个情节需要删除")

            # 删除所有关联的情节及其关系
            delete_query = """
            MATCH (p:Plot)
            WHERE p.id IN $plot_ids
            OPTIONAL MATCH (p)-[r]-()
            DELETE r, p
            """

            session.run(delete_query, plot_ids=plot_ids)

            logger.info(f"已删除提取任务 {extraction_task_id} 的 {len(plot_ids)} 个情节")
            return len(plot_ids)

    def delete_plot(self, plot_id: str) -> bool:
        """
        删除单个情节

        Args:
            plot_id: 情节ID

        Returns:
            是否删除成功
        """
        # 确保连接可用
        self._ensure_connection()

        try:
            with self.driver.session() as session:
                # 删除情节及其所有关系
                delete_query = """
                MATCH (p:Plot {id: $plot_id})
                OPTIONAL MATCH (p)-[r]-()
                DELETE r, p
                RETURN count(p) as deleted_count
                """

                result = session.run(delete_query, plot_id=plot_id)
                deleted_count = result.single()['deleted_count']

                if deleted_count > 0:
                    logger.info(f"已删除情节: {plot_id}")
                    return True
                else:
                    logger.warning(f"未找到情节: {plot_id}")
                    return False

        except Exception as e:
            logger.error(f"删除情节失败: {e}")
            return False

    @retry_on_defunct_connection(max_retries=2, delay=0.5)
    def get_plot_detail(self, plot_id: str) -> Optional[Dict]:
        """获取情节详情"""
        # 确保连接可用
        self._ensure_connection()

        query = """
        MATCH (p:Plot {id: $plot_id})
        OPTIONAL MATCH (p)-[:HAPPENS_IN]->(c:Chapter)
        OPTIONAL MATCH (e:Event)-[:PART_OF]->(p)
        OPTIONAL MATCH (ch:Character)-[:PARTICIPATES_IN]->(p)
        RETURN p,
               COLLECT(DISTINCT c.id) as chapter_ids,
               COLLECT(DISTINCT e.id) as event_ids,
               COLLECT(DISTINCT ch.name) as character_names
        """

        with self.driver.session() as session:
            result = session.run(query, plot_id=plot_id)
            record = result.single()
            if record:
                plot_data = dict(record['p'])
                plot_data['chapter_ids'] = record['chapter_ids']
                plot_data['event_ids'] = record['event_ids']
                plot_data['character_names'] = record['character_names']
                return plot_data
            return None

    @retry_on_defunct_connection(max_retries=2, delay=0.5)
    def get_plot_events(self, plot_id: str) -> List[Dict]:
        """获取情节包含的所有事件"""
        # 确保连接可用
        self._ensure_connection()

        # 先获取情节的章节范围和小说ID
        plot_query = """
        MATCH (p:Plot {id: $plot_id})
        RETURN p.start_chapter as start_chapter,
               p.end_chapter as end_chapter,
               p.novel_id as novel_id
        """

        with self.driver.session() as session:
            plot_result = session.run(plot_query, plot_id=plot_id)
            plot_record = plot_result.single()

            if not plot_record:
                return []

            start_chapter = plot_record['start_chapter']
            end_chapter = plot_record['end_chapter']
            novel_id = plot_record['novel_id']

            # 如果没有章节范围，返回空列表
            if start_chapter is None or end_chapter is None:
                return []

            # 查询该章节范围内的所有事件
            events_query = """
            MATCH (e:Event)
            WHERE e.novel_id = $novel_id
              AND e.chapter_number >= $start_chapter
              AND e.chapter_number <= $end_chapter
            RETURN e
            ORDER BY e.chapter_number
            """

            result = session.run(
                events_query,
                novel_id=novel_id,
                start_chapter=start_chapter,
                end_chapter=end_chapter
            )

            events = []
            for record in result:
                event_data = dict(record['e'])
                events.append(event_data)

            return events


# 全局知识图谱服务实例（延迟初始化）
kg_service = None

def get_kg_service():
    """获取知识图谱服务实例（懒加载）"""
    global kg_service
    if kg_service is None:
        try:
            kg_service = KnowledgeGraphService()
            logger.debug("知识图谱服务初始化成功")
        except Exception as e:
            logger.error(f"知识图谱服务初始化失败: {e}")
            raise e
    return kg_service


def serialize_neo4j_types(data: Any) -> Any:
    """
    将Neo4j数据类型序列化为Python原生类型，以便JSON序列化

    Args:
        data: Neo4j返回的数据，可能包含Node、Relationship等特殊类型

    Returns:
        序列化后的数据
    """
    from neo4j.graph import Node, Relationship
    from neo4j.time import DateTime, Date, Time
    from datetime import datetime, date, time

    if data is None:
        return None

    # 处理Neo4j Node类型
    if isinstance(data, Node):
        result = dict(data)
        result['_id'] = data.id
        result['_labels'] = list(data.labels)
        return serialize_neo4j_types(result)

    # 处理Neo4j Relationship类型
    if isinstance(data, Relationship):
        result = {
            '_id': data.id,
            '_type': data.type,
            '_start_node_id': data.start_node.id,
            '_end_node_id': data.end_node.id,
            **dict(data)
        }
        return serialize_neo4j_types(result)

    # 处理Neo4j时间类型
    if isinstance(data, DateTime):
        return data.to_native().isoformat()
    if isinstance(data, Date):
        return data.to_native().isoformat()
    if isinstance(data, Time):
        return data.to_native().isoformat()

    # 处理Python datetime类型
    if isinstance(data, (datetime, date, time)):
        return data.isoformat()

    # 处理字典
    if isinstance(data, dict):
        return {key: serialize_neo4j_types(value) for key, value in data.items()}

    # 处理列表
    if isinstance(data, (list, tuple)):
        return [serialize_neo4j_types(item) for item in data]

    # 处理集合
    if isinstance(data, set):
        return [serialize_neo4j_types(item) for item in data]

    # 处理bytes
    if isinstance(data, bytes):
        return data.decode('utf-8', errors='ignore')

    # 其他类型直接返回
    return data