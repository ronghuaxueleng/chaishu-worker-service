import json
import httpx
import requests
import asyncio
import traceback
from typing import Dict, List, Optional, Any, Tuple
from abc import ABC, abstractmethod
from datetime import datetime
import logging
import time
from src.services.database_service import db_service

# 配置日志
logger = logging.getLogger(__name__)

# Redis配置相关常量
AI_CONFIG_VERSION_KEY = "ai_config:version"
AI_CONFIG_DATA_KEY = "ai_config:providers"

# 设置第三方库的日志级别
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

class AIServiceException(Exception):
    """AI服务异常"""
    def __init__(self, message: str, provider: str = None, error_code: str = None):
        self.message = message
        self.provider = provider
        self.error_code = error_code
        super().__init__(self.message)

class BaseAIService(ABC):
    """AI服务基类"""
    
    def __init__(self, api_key: str, base_url: str, models: List[str]):
        self.api_key = api_key
        self.base_url = base_url
        self.models = models
        self.client = httpx.AsyncClient(timeout=60.0)
        self._client_closed = False
    
    @abstractmethod
    async def generate_response(self, prompt: str, model: str, **kwargs) -> Dict[str, Any]:
        """生成AI响应"""
        pass
    
    @abstractmethod
    def calculate_tokens(self, text: str) -> int:
        """计算Token数量"""
        pass
    
    @abstractmethod
    def estimate_cost(self, input_tokens: int, output_tokens: int, model: str) -> float:
        """估算成本"""
        pass
    
    async def test_connection(self, model: str) -> bool:
        """测试连接"""
        try:
            await self.ensure_client_available()
            response = await self.generate_response("测试连接", model, max_tokens=10)
            return response.get('success', False)
        except Exception as e:
            logger.warning(f"连接测试失败: {str(e)}")
            return False
    
    async def __aenter__(self):
        # 确保客户端可用
        if self._client_closed or self.client.is_closed:
            try:
                await self.client.aclose()
            except:
                pass
            self.client = httpx.AsyncClient(timeout=60.0)
            self._client_closed = False
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if not self._client_closed and not self.client.is_closed:
                await self.client.aclose()
        except:
            pass
        finally:
            self._client_closed = True

    async def ensure_client_available(self):
        """确保客户端可用"""
        if self._client_closed or self.client.is_closed:
            try:
                await self.client.aclose()
            except:
                pass
            self.client = httpx.AsyncClient(timeout=60.0)
            self._client_closed = False

    async def close(self):
        """关闭客户端"""
        try:
            if not self._client_closed and not self.client.is_closed:
                await self.client.aclose()
        except:
            pass
        finally:
            self._client_closed = True

class OpenAIService(BaseAIService):
    """OpenAI服务"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.openai.com/v1"):
        models = ["gpt-4", "gpt-4-turbo", "gpt-3.5-turbo", "gpt-4o", "gpt-4o-mini"]
        super().__init__(api_key, base_url, models)
        
        # Token定价 (per 1K tokens)
        self.pricing = {
            "gpt-4": {"input": 0.03, "output": 0.06},
            "gpt-4-turbo": {"input": 0.01, "output": 0.03},
            "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
            "gpt-4o": {"input": 0.005, "output": 0.015},
            "gpt-4o-mini": {"input": 0.00015, "output": 0.0006}
        }
    
    async def generate_response(self, prompt: str, model: str, **kwargs) -> Dict[str, Any]:
        """生成OpenAI响应"""
        try:
            await self.ensure_client_available()
            
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": kwargs.get('max_tokens', 2000),
                "temperature": kwargs.get('temperature', 0.7)
            }
            
            url = f"{self.base_url}/chat/completions"
            response = await self.client.post(url, headers=headers, json=data)
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"OpenAI响应成功 - 输入tokens: {result['usage']['prompt_tokens']}, 输出tokens: {result['usage']['completion_tokens']}")
                return {
                    "success": True,
                    "content": result["choices"][0]["message"]["content"],
                    "input_tokens": result["usage"]["prompt_tokens"],
                    "output_tokens": result["usage"]["completion_tokens"],
                    "total_tokens": result["usage"]["total_tokens"]
                }
            else:
                error_data = response.json()
                logger.error(f"OpenAI API错误 - 状态码: {response.status_code}, 错误: {error_data}")
                logger.error(f"OpenAI错误响应文本: {response.text}")
                raise AIServiceException(
                    f"OpenAI API错误: {error_data.get('error', {}).get('message', '未知错误')}",
                    provider="openai",
                    error_code=str(response.status_code)
                )
        
        except httpx.RequestError as e:
            logger.error(f"OpenAI网络请求错误: {str(e)}")
            raise AIServiceException(f"网络请求错误: {str(e)}", provider="openai")
        except Exception as e:
            logger.error(f"OpenAI服务异常: {str(e)}")
            raise AIServiceException(f"OpenAI服务错误: {str(e)}", provider="openai")
    
    def calculate_tokens(self, text: str) -> int:
        """简单的Token计算（实际应该使用tiktoken）"""
        return len(text) // 4  # 粗略估算，1个token约等于4个字符
    
    def estimate_cost(self, input_tokens: int, output_tokens: int, model: str) -> float:
        """估算成本"""
        if model not in self.pricing:
            return 0.0
        
        input_cost = (input_tokens / 1000) * self.pricing[model]["input"]
        output_cost = (output_tokens / 1000) * self.pricing[model]["output"]
        return input_cost + output_cost

class ClaudeService(BaseAIService):
    """Claude服务"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.anthropic.com/v1"):
        models = ["claude-3-opus", "claude-3-sonnet", "claude-3-haiku", "claude-3-5-sonnet"]
        super().__init__(api_key, base_url, models)
        
        # Token定价 (per 1K tokens)
        self.pricing = {
            "claude-3-opus": {"input": 0.015, "output": 0.075},
            "claude-3-sonnet": {"input": 0.003, "output": 0.015},
            "claude-3-haiku": {"input": 0.00025, "output": 0.00125},
            "claude-3-5-sonnet": {"input": 0.003, "output": 0.015}
        }
    
    async def generate_response(self, prompt: str, model: str, **kwargs) -> Dict[str, Any]:
        """生成Claude响应"""
        try:
            headers = {
                "x-api-key": self.api_key,
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            data = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": kwargs.get('max_tokens', 2000),
                "temperature": kwargs.get('temperature', 0.7)
            }
            
            response = await self.client.post(
                f"{self.base_url}/messages",
                headers=headers,
                json=data
            )
            
            if response.status_code == 200:
                result = response.json()
                return {
                    "success": True,
                    "content": result["content"][0]["text"],
                    "input_tokens": result["usage"]["input_tokens"],
                    "output_tokens": result["usage"]["output_tokens"],
                    "total_tokens": result["usage"]["input_tokens"] + result["usage"]["output_tokens"]
                }
            else:
                error_data = response.json()
                raise AIServiceException(
                    f"Claude API错误: {error_data.get('error', {}).get('message', '未知错误')}",
                    provider="claude",
                    error_code=str(response.status_code)
                )
        
        except httpx.RequestError as e:
            raise AIServiceException(f"网络请求错误: {str(e)}", provider="claude")
        except Exception as e:
            logger.error(f"Claude服务异常: {str(e)}")
            raise AIServiceException(f"Claude服务错误: {str(e)}", provider="claude")
    
    def calculate_tokens(self, text: str) -> int:
        """简单的Token计算"""
        return len(text) // 4
    
    def estimate_cost(self, input_tokens: int, output_tokens: int, model: str) -> float:
        """估算成本"""
        if model not in self.pricing:
            return 0.0
        
        input_cost = (input_tokens / 1000) * self.pricing[model]["input"]
        output_cost = (output_tokens / 1000) * self.pricing[model]["output"]
        return input_cost + output_cost

class ZhipuAIService(BaseAIService):
    """智谱AI服务"""
    
    def __init__(self, api_key: str, base_url: str = "https://open.bigmodel.cn/api/paas/v4"):
        models = ["glm-4", "glm-4v", "glm-3-turbo"]
        super().__init__(api_key, base_url, models)
        
        # Token定价 (per 1K tokens)
        self.pricing = {
            "glm-4": {"input": 0.01, "output": 0.01},
            "glm-4v": {"input": 0.01, "output": 0.01},
            "glm-3-turbo": {"input": 0.005, "output": 0.005}
        }
    
    async def generate_response(self, prompt: str, model: str, **kwargs) -> Dict[str, Any]:
        """生成智谱AI响应"""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": kwargs.get('max_tokens', 2000),
                "temperature": kwargs.get('temperature', 0.7)
            }
            
            response = await self.client.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=data
            )
            
            if response.status_code == 200:
                result = response.json()
                return {
                    "success": True,
                    "content": result["choices"][0]["message"]["content"],
                    "input_tokens": result["usage"]["prompt_tokens"],
                    "output_tokens": result["usage"]["completion_tokens"],
                    "total_tokens": result["usage"]["total_tokens"]
                }
            else:
                error_data = response.json()
                raise AIServiceException(
                    f"智谱AI API错误: {error_data.get('error', {}).get('message', '未知错误')}",
                    provider="zhipu",
                    error_code=str(response.status_code)
                )
        
        except httpx.RequestError as e:
            raise AIServiceException(f"网络请求错误: {str(e)}", provider="zhipu")
        except Exception as e:
            logger.error(f"智谱AI服务错误: {str(e)}")
            raise AIServiceException(f"智谱AI服务错误: {str(e)}", provider="zhipu")
    
    def calculate_tokens(self, text: str) -> int:
        """简单的Token计算"""
        return len(text) // 2  # 中文token密度更高
    
    def estimate_cost(self, input_tokens: int, output_tokens: int, model: str) -> float:
        """估算成本"""
        if model not in self.pricing:
            return 0.0
        
        input_cost = (input_tokens / 1000) * self.pricing[model]["input"]
        output_cost = (output_tokens / 1000) * self.pricing[model]["output"]
        return input_cost + output_cost

class DeepSeekService(BaseAIService):
    """DeepSeek服务"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.deepseek.com/v1"):
        models = ["deepseek-chat", "deepseek-coder"]
        super().__init__(api_key, base_url, models)
        
        # Token定价 (per 1K tokens)
        self.pricing = {
            "deepseek-chat": {"input": 0.0014, "output": 0.0028},
            "deepseek-coder": {"input": 0.0014, "output": 0.0028}
        }
    
    async def generate_response(self, prompt: str, model: str, **kwargs) -> Dict[str, Any]:
        """生成DeepSeek响应"""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": kwargs.get('max_tokens', 2000),
                "temperature": kwargs.get('temperature', 0.7)
            }
            
            response = await self.client.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=data
            )
            
            if response.status_code == 200:
                result = response.json()
                return {
                    "success": True,
                    "content": result["choices"][0]["message"]["content"],
                    "input_tokens": result["usage"]["prompt_tokens"],
                    "output_tokens": result["usage"]["completion_tokens"],
                    "total_tokens": result["usage"]["total_tokens"]
                }
            else:
                error_data = response.json()
                raise AIServiceException(
                    f"DeepSeek API错误: {error_data.get('error', {}).get('message', '未知错误')}",
                    provider="deepseek",
                    error_code=str(response.status_code)
                )
        
        except httpx.RequestError as e:
            raise AIServiceException(f"网络请求错误: {str(e)}", provider="deepseek")
        except Exception as e:
            logger.error(f"DeepSeek服务错误: {str(e)}")
            raise AIServiceException(f"DeepSeek服务错误: {str(e)}", provider="deepseek")
    
    def calculate_tokens(self, text: str) -> int:
        """简单的Token计算"""
        return len(text) // 4
    
    def estimate_cost(self, input_tokens: int, output_tokens: int, model: str) -> float:
        """估算成本"""
        if model not in self.pricing:
            return 0.0
        
        input_cost = (input_tokens / 1000) * self.pricing[model]["input"]
        output_cost = (output_tokens / 1000) * self.pricing[model]["output"]
        return input_cost + output_cost

class OllamaService(BaseAIService):
    """本地Ollama服务"""
    
    def __init__(self, api_key: str = None, base_url: str = "http://localhost:11434"):
        # Ollama不需要API密钥，但保留参数保持接口一致性
        models = []  # 动态获取可用模型
        super().__init__(api_key or "", base_url, models)
        
        # Ollama本地服务无成本，但保留结构
        self.pricing = {}
    
    async def get_available_models(self) -> List[str]:
        """获取Ollama可用模型列表"""
        try:
            response = await self.client.get(f"{self.base_url}/api/tags")
            if response.status_code == 200:
                data = response.json()
                models = [model['name'] for model in data.get('models', [])]
                logger.info(f"Ollama可用模型: {models}")
                return models
            else:
                logger.error(f"Ollama获取模型列表失败: {response.status_code}")
                return []
        except Exception as e:
            logger.warning(f"Ollama连接失败: {str(e)}")
            return []
    
    async def generate_response(self, prompt: str, model: str, **kwargs) -> Dict[str, Any]:
        """生成Ollama响应"""
        try:
            data = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": kwargs.get('temperature', 0.7),
                    "num_predict": kwargs.get('max_tokens', 2000)
                }
            }
            
            response = await self.client.post(
                f"{self.base_url}/api/generate",
                json=data
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # 估算token数量（Ollama可能不返回token统计）
                input_tokens = self.calculate_tokens(prompt)
                output_tokens = self.calculate_tokens(result.get('response', ''))
                
                logger.info(f"Ollama响应成功 - 估算输入tokens: {input_tokens}, 输出tokens: {output_tokens}")
                
                return {
                    "success": True,
                    "content": result.get('response', ''),
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": input_tokens + output_tokens,
                    "model_info": result.get('model', model)
                }
            else:
                error_msg = f"状态码: {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg = error_data.get('error', error_msg)
                except:
                    pass
                
                logger.error(f"Ollama API错误 - 状态码: {response.status_code}, 错误: {error_msg}")
                raise AIServiceException(
                    f"Ollama API错误: {error_msg}",
                    provider="ollama",
                    error_code=str(response.status_code)
                )
        
        except httpx.RequestError as e:
            logger.error(f"Ollama网络请求错误: {str(e)}")
            raise AIServiceException(f"网络请求错误: {str(e)}", provider="ollama")
        except Exception as e:
            logger.error(f"Ollama服务异常: {str(e)}")
            raise AIServiceException(f"Ollama服务错误: {str(e)}", provider="ollama")
    
    def calculate_tokens(self, text: str) -> int:
        """简单的Token计算（中文优化）"""
        # 中文字符和英文单词的token密度不同
        chinese_chars = len([c for c in text if '\u4e00' <= c <= '\u9fff'])
        other_chars = len(text) - chinese_chars
        return chinese_chars + (other_chars // 4)
    
    def estimate_cost(self, input_tokens: int, output_tokens: int, model: str) -> float:
        """本地服务无成本"""
        return 0.0
    
    async def test_connection(self, model: str = None) -> bool:
        """测试Ollama连接"""
        try:
            # 先测试服务是否可用
            response = await self.client.get(f"{self.base_url}/api/tags")
            if response.status_code != 200:
                return False
            
            # 如果指定了模型，测试模型是否可用
            if model:
                available_models = await self.get_available_models()
                return model in available_models
            
            return True
        except Exception as e:
            logger.warning(f"Ollama连接测试失败: {str(e)}")
            return False

class LocalAIService(BaseAIService):
    """本地LocalAI服务（OpenAI兼容接口）"""
    
    def __init__(self, api_key: str = "not-needed", base_url: str = "http://localhost:8080"):
        models = []  # 动态获取模型
        super().__init__(api_key, base_url, models)
        
        # LocalAI本地服务无成本
        self.pricing = {}
    
    async def get_available_models(self) -> List[str]:
        """获取LocalAI可用模型列表"""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            response = await self.client.get(
                f"{self.base_url}/v1/models",
                headers=headers
            )
            if response.status_code == 200:
                data = response.json()
                models = [model['id'] for model in data.get('data', [])]
                logger.info(f"LocalAI可用模型: {models}")
                return models
            else:
                logger.error(f"LocalAI获取模型列表失败: {response.status_code}")
                return []
        except Exception as e:
            logger.warning(f"LocalAI连接失败: {str(e)}")
            return []
    
    async def generate_response(self, prompt: str, model: str, **kwargs) -> Dict[str, Any]:
        """生成LocalAI响应（使用OpenAI兼容接口）"""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": kwargs.get('max_tokens', 2000),
                "temperature": kwargs.get('temperature', 0.7),
                "stream": False
            }
            
            response = await self.client.post(
                f"{self.base_url}/v1/chat/completions",
                headers=headers,
                json=data
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # LocalAI可能不返回token统计，使用估算
                usage = result.get('usage', {})
                input_tokens = usage.get('prompt_tokens') or self.calculate_tokens(prompt)
                output_tokens = usage.get('completion_tokens') or self.calculate_tokens(
                    result.get("choices", [{}])[0].get("message", {}).get("content", "")
                )
                
                logger.info(f"LocalAI响应成功 - 输入tokens: {input_tokens}, 输出tokens: {output_tokens}")
                
                return {
                    "success": True,
                    "content": result["choices"][0]["message"]["content"],
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": input_tokens + output_tokens
                }
            else:
                error_data = response.json() if response.content else {}
                logger.error(f"LocalAI API错误 - 状态码: {response.status_code}, 错误: {error_data}")
                raise AIServiceException(
                    f"LocalAI API错误: {error_data.get('error', {}).get('message', '未知错误')}",
                    provider="localai",
                    error_code=str(response.status_code)
                )
        
        except httpx.RequestError as e:
            logger.error(f"LocalAI网络请求错误: {str(e)}")
            raise AIServiceException(f"网络请求错误: {str(e)}", provider="localai")
        except Exception as e:
            logger.error(f"LocalAI服务异常: {str(e)}")
            raise AIServiceException(f"LocalAI服务错误: {str(e)}", provider="localai")
    
    def calculate_tokens(self, text: str) -> int:
        """简单的Token计算"""
        chinese_chars = len([c for c in text if '\u4e00' <= c <= '\u9fff'])
        other_chars = len(text) - chinese_chars
        return chinese_chars + (other_chars // 4)
    
    def estimate_cost(self, input_tokens: int, output_tokens: int, model: str) -> float:
        """本地服务无成本"""
        return 0.0

class OpenAICompatibleService(BaseAIService):
    """通用OpenAI兼容接口服务"""

    def __init__(self, api_key: str = "not-needed", base_url: str = "http://localhost:8000", provider_name: str = "openai-compatible"):
        models = []  # 动态获取模型
        super().__init__(api_key, base_url, models)

        # 存储服务商名称，用于日志输出
        self.provider_name = provider_name

        # 通用本地服务无成本
        self.pricing = {}
    
    async def get_available_models(self) -> List[str]:
        """获取可用模型列表"""
        try:
            await self.ensure_client_available()

            headers = {
                "Content-Type": "application/json"
            }

            # 只有当API key不为空时才添加Authorization头
            if self.api_key and self.api_key.strip():
                headers["Authorization"] = f"Bearer {self.api_key}"
            response = await self.client.get(
                f"{self.base_url}/v1/models",
                headers=headers
            )
            if response.status_code == 200:
                data = response.json()
                models = [model['id'] for model in data.get('data', [])]
                logger.info(f"OpenAI兼容服务可用模型: {models}")
                return models
            else:
                logger.error(f"OpenAI兼容服务获取模型列表失败: {response.status_code}")
                return []
        except Exception as e:
            logger.warning(f"OpenAI兼容服务连接失败: {str(e)}")
            return []

    async def generate_response(self, prompt: str, model: str, is_global: bool = False, **kwargs) -> Dict[str, Any]:
        """生成OpenAI兼容响应（异步包装，调用同步实现）"""
        # 直接调用同步方法，因为内部使用requests而非httpx
        return self.generate_response_sync(prompt, model, is_global=is_global, **kwargs)

    def generate_response_sync(self, prompt: str, model: str, is_global: bool = False, **kwargs) -> Dict[str, Any]:
        """生成OpenAI兼容响应（同步版本）"""
        try:
            headers = {
                "Content-Type": "application/json"
            }

            # 只有当API key不为空时才添加Authorization头
            if self.api_key and self.api_key.strip():
                headers["Authorization"] = f"Bearer {self.api_key}"

            # 使用更简单的payload，匹配成功的调用方式
            data = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": kwargs.get('temperature', 0.7)
            }

            # 只在明确指定时才添加max_tokens
            if 'max_tokens' in kwargs:
                data["max_tokens"] = kwargs['max_tokens']

            # 根据是否全局步骤调整超时时间
            timeout = 120 if is_global else 120  # 默认都设置为2分钟

            url = f"{self.base_url}/v1/chat/completions"

            # 调试日志：输出请求详情
            logger.info(f"[{self.provider_name}] OpenAI兼容API请求 - URL: {url}, Model: {model}, Prompt长度: {len(prompt)}")

            # 使用同步requests
            response = requests.post(url, headers=headers, json=data, timeout=timeout)
            
            if response.status_code == 200:
                # 使用requests处理响应
                try:
                    result = response.json()
                except Exception as e:
                    logger.error(f"OpenAI兼容API响应解析失败: {response.text}")
                    raise AIServiceException(f"响应解析失败: {str(e)}", provider="openai-compatible")
                
                # 检查响应格式，确保包含choices字段
                if "choices" not in result or not result["choices"]:
                    logger.error(f"OpenAI兼容API响应格式错误: {result}")
                    raise AIServiceException("API响应缺少choices字段", provider="openai-compatible")
                
                # 尝试获取token统计，没有则估算
                usage = result.get('usage', {})
                input_tokens = usage.get('prompt_tokens') or self.calculate_tokens(prompt)
                
                # 安全获取响应内容
                content = ""
                try:
                    content = result["choices"][0]["message"]["content"]
                except (KeyError, IndexError, TypeError) as e:
                    logger.error(f"OpenAI兼容API响应结构错误: {e}, 响应: {result}")
                    raise AIServiceException(f"API响应结构错误: {e}", provider="openai-compatible")
                
                # 验证内容有效性
                if not content or content.strip() == "":
                    logger.error(f"OpenAI兼容API返回空内容 - 响应: {result}")
                    raise AIServiceException("API返回空内容，可能是模型配置问题或API限制", provider="openai-compatible")
                
                output_tokens = usage.get('completion_tokens') or self.calculate_tokens(content)

                logger.info(f"[{self.provider_name}] OpenAI兼容响应成功 - 输入tokens: {input_tokens}, 输出tokens: {output_tokens}, 内容长度: {len(content)}")
                
                return {
                    "success": True,
                    "content": content,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": input_tokens + output_tokens
                }
            else:
                try:
                    error_data = response.json() if response.text else {}
                except:
                    error_data = {}
                logger.error(f"OpenAI兼容API错误 - 状态码: {response.status_code}, 错误: {error_data}")
                logger.error(f"OpenAI兼容错误响应文本: {response.text}")
                raise AIServiceException(
                    f"OpenAI兼容API错误: {error_data.get('error', {}).get('message', '未知错误')}",
                    provider="openai-compatible",
                    error_code=str(response.status_code)
                )
        
        except requests.ConnectionError as e:
            logger.error(f"OpenAI兼容连接错误: {str(e)}")
            raise AIServiceException(f"连接失败", provider="openai-compatible")
        except requests.Timeout as e:
            logger.error(f"OpenAI兼容超时错误: {str(e)}")
            raise AIServiceException(f"请求超时", provider="openai-compatible")
        except requests.RequestException as e:
            logger.error(f"OpenAI兼容网络请求错误: {str(e)}")
            raise AIServiceException(f"网络请求错误: {str(e)}", provider="openai-compatible")
        except Exception as e:
            # 特殊处理事件循环关闭的情况（通常是用户停止任务导致）
            if "Event loop is closed" in str(e) or "RuntimeError" in str(type(e).__name__):
                logger.warning(f"检测到事件循环关闭（可能是用户停止了任务）: {str(e)}")
                raise AIServiceException("任务已被停止", provider="openai-compatible")
            
            logger.error(f"OpenAI兼容服务异常: {str(e)}")
            raise AIServiceException(f"OpenAI兼容服务错误: {str(e)}", provider="openai-compatible")
    
    def _parse_stream_response(self, response_text: str) -> dict:
        """解析流式响应"""
        import json
        
        lines = response_text.strip().split('\n')
        content_parts = []
        usage_info = {}
        
        for line in lines:
            if line.startswith('data: '):
                data_str = line[6:]  # 去掉'data: '前缀
                if data_str.strip() == '[DONE]':
                    break
                try:
                    data_obj = json.loads(data_str)
                    if 'choices' in data_obj and data_obj['choices']:
                        choice = data_obj['choices'][0]
                        delta = choice.get('delta', {})
                        if 'content' in delta and delta['content']:
                            content_parts.append(delta['content'])
                    
                    # 收集usage信息
                    if 'usage' in data_obj:
                        usage_info = data_obj['usage']
                        
                except json.JSONDecodeError:
                    continue
        
        # 构造标准响应格式
        full_content = ''.join(content_parts)
        return {
            'choices': [{
                'message': {
                    'content': full_content,
                    'role': 'assistant'
                },
                'finish_reason': 'stop'
            }],
            'usage': usage_info or {
                'prompt_tokens': 1,
                'completion_tokens': 1,
                'total_tokens': 2
            }
        }
    
    def calculate_tokens(self, text: str) -> int:
        """简单的Token计算"""
        chinese_chars = len([c for c in text if '\u4e00' <= c <= '\u9fff'])
        other_chars = len(text) - chinese_chars
        return chinese_chars + (other_chars // 4)
    
    def estimate_cost(self, input_tokens: int, output_tokens: int, model: str) -> float:
        """本地服务无成本"""
        return 0.0

def sync_ai_config_to_redis():
    """将AI服务商配置同步到Redis"""
    try:
        from src.utils.redis_client import get_redis_client
        redis_client = get_redis_client()
        if not redis_client:
            logger.warning("Redis未连接，无法同步AI配置")
            return False

        # 获取所有AI服务商配置
        providers = db_service.get_all_ai_providers()
        providers_data = []
        for provider in providers:
            providers_data.append({
                'name': provider.name,
                'api_key': provider.api_key,
                'base_url': provider.base_url,
                'models': provider.models,
                'is_active': provider.is_active
            })

        # 生成新版本号（使用时间戳）
        version = int(time.time() * 1000)  # 毫秒时间戳

        # 保存到Redis
        redis_client.set(AI_CONFIG_DATA_KEY, providers_data)
        redis_client.set(AI_CONFIG_VERSION_KEY, version)

        logger.info(f"AI配置已同步到Redis，版本号: {version}, 服务商数量: {len(providers_data)}")
        return True

    except Exception as e:
        logger.error(f"同步AI配置到Redis失败: {e}")
        return False


def get_ai_config_from_redis():
    """从Redis获取AI服务商配置

    Returns:
        tuple: (providers_data, version) 或 (None, None)
    """
    try:
        from src.utils.redis_client import get_redis_client
        redis_client = get_redis_client()
        if not redis_client:
            return None, None

        version = redis_client.get(AI_CONFIG_VERSION_KEY)
        providers_data = redis_client.get(AI_CONFIG_DATA_KEY)

        if version is None or providers_data is None:
            logger.debug("Redis中没有AI配置，将从数据库加载")
            return None, None

        logger.debug(f"从Redis获取AI配置，版本号: {version}, 服务商数量: {len(providers_data)}")
        return providers_data, version

    except Exception as e:
        logger.error(f"从Redis获取AI配置失败: {e}")
        return None, None


class AIServiceManager:
    """AI服务管理器"""

    def __init__(self):
        self.services = {}
        self.config_version = None  # 当前加载的配置版本
        self.load_services()
    
    def load_services(self):
        """加载所有AI服务（优先从Redis加载）"""
        logger.info("开始加载AI服务")

        # 尝试从Redis加载配置
        providers_data, version = get_ai_config_from_redis()

        if providers_data is not None and version is not None:
            # 从Redis加载成功
            logger.info(f"从Redis加载AI配置，版本号: {version}")
            self.config_version = version
            self._load_services_from_data(providers_data)
        else:
            # Redis中没有配置，从数据库加载
            logger.info("从数据库加载AI配置")
            providers = db_service.get_all_ai_providers()

            # 转换为配置数据格式
            providers_data = []
            for provider in providers:
                providers_data.append({
                    'name': provider.name,
                    'api_key': provider.api_key,
                    'base_url': provider.base_url,
                    'models': provider.models,
                    'is_active': provider.is_active
                })

            self._load_services_from_data(providers_data)

            # 同步到Redis
            if sync_ai_config_to_redis():
                # 获取刚才同步的版本号
                _, self.config_version = get_ai_config_from_redis()

    def _load_services_from_data(self, providers_data: List[Dict]):
        """从配置数据加载服务"""
        for provider_data in providers_data:
            name = provider_data['name']
            api_key = provider_data.get('api_key')
            base_url = provider_data.get('base_url')

            # 本地服务可能不需要API密钥
            if not api_key and name not in ["ollama", "localai", "openai-compatible"]:
                logger.warning(f"AI服务商 [{name}] 缺少API密钥，跳过加载")
                continue

            try:
                if name == "openai":
                    self.services[name] = OpenAIService(api_key, base_url)
                elif name == "claude":
                    self.services[name] = ClaudeService(api_key, base_url)
                elif name == "zhipu":
                    self.services[name] = ZhipuAIService(api_key, base_url)
                elif name == "deepseek":
                    self.services[name] = DeepSeekService(api_key, base_url)
                elif name == "ollama":
                    self.services[name] = OllamaService(api_key, base_url)
                elif name == "localai":
                    self.services[name] = LocalAIService(api_key, base_url)
                elif name == "openai-compatible":
                    self.services[name] = OpenAICompatibleService(api_key, base_url, provider_name=name)
                else:
                    # 兼容未知命名的服务商
                    if base_url:
                        self.services[name] = OpenAICompatibleService(api_key or "", base_url, provider_name=name)
                        logger.info(f"以OpenAI兼容模式加载AI服务: {name} -> {base_url}")
                    else:
                        logger.warning(f"未知AI服务商且缺少base_url，跳过: {name}")

                logger.info(f"成功加载AI服务: {name}")
            except Exception as e:
                logger.error(f"加载AI服务失败 [{name}]: {str(e)}")
    
    def get_service(self, provider_name: str) -> Optional[BaseAIService]:
        """获取AI服务"""
        return self.services.get(provider_name)
    
    def get_available_services(self) -> List[str]:
        """获取可用的AI服务列表"""
        return list(self.services.keys())
    
    async def test_service(self, provider_name: str, model: str) -> bool:
        """测试AI服务"""
        service = self.get_service(provider_name)
        if not service:
            return False
        
        return await service.test_connection(model)
    
    async def generate_analysis(self, provider_name: str, model: str, prompt: str, 
                              is_global: bool = False, **kwargs) -> Dict[str, Any]:
        """生成AI分析"""
        service = self.get_service(provider_name)
        if not service:
            logger.error(f"AI服务不可用: {provider_name}")
            raise AIServiceException(f"AI服务不可用: {provider_name}")
        
        # 将is_global参数传递给具体的服务实现
        result = await service.generate_response(prompt, model, is_global=is_global, **kwargs)
        if result.get('success'):
            logger.info(f"AI分析完成 - 服务商: {provider_name}, 输出长度: {len(result.get('content', ''))}")
        else:
            logger.error(f"AI分析失败 - 服务商: {provider_name}")
        
        return result
    
    def generate_analysis_sync(self, provider_name: str, model: str, prompt: str, 
                              **kwargs) -> Dict[str, Any]:
        """生成AI分析（同步版本）"""
        import asyncio
        
        service = self.get_service(provider_name)
        if not service:
            logger.error(f"AI服务不可用: {provider_name}")
            raise AIServiceException(f"AI服务不可用: {provider_name}")
        
        # 在新的事件循环中运行异步方法
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(service.generate_response(prompt, model, **kwargs))
                if result.get('success'):
                    logger.info(f"AI分析完成 - 服务商: {provider_name}, 输出长度: {len(result.get('content', ''))}")
                else:
                    logger.error(f"AI分析失败 - 服务商: {provider_name}")
                return result
            finally:
                # 清理事件循环前先关闭HTTP客户端
                try:
                    if hasattr(service, 'client') and service.client:
                        loop.run_until_complete(service.client.aclose())
                except Exception:
                    pass
                
                # 清理事件循环
                pending_tasks = asyncio.all_tasks(loop)
                if pending_tasks:
                    for task in pending_tasks:
                        task.cancel()
                    try:
                        loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
                    except Exception:
                        pass
                loop.close()
        except Exception as e:
            logger.error(f"同步AI分析异常: {str(e)}")
            raise AIServiceException(f"同步AI分析异常: {str(e)}")
    
    def generate_response(self, prompt: str, provider_name: str, model_name: str, **kwargs) -> Dict[str, Any]:
        """生成AI响应（同步版本）"""
        from src.services.ai_provider_throttle import is_suspended, increment_failure, reset_failures

        service = self.get_service(provider_name)
        if not service:
            logger.error(f"AI服务不可用: {provider_name}")
            return {"success": False, "error": f"AI服务不可用: {provider_name}"}

        # 服务商是否被临时暂停
        try:
            if is_suspended(provider_name):
                logger.warning(f"AI服务商暂时暂停使用: {provider_name}")
                return {"success": False, "error": f"AI服务商[{provider_name}]暂时暂停使用"}
        except Exception:
            pass

        # 对于OpenAICompatibleService，直接调用同步方法，避免事件循环开销
        if isinstance(service, OpenAICompatibleService):
            try:
                result = service.generate_response_sync(prompt, model_name, **kwargs)
                if result.get('success'):
                    logger.info(f"AI响应完成 - 服务商: {provider_name}, 输出长度: {len(result.get('content', ''))}")
                    # 成功时重置失败计数
                    try:
                        reset_failures(provider_name)
                    except Exception:
                        pass
                    return {"success": True, "response": result.get('content', '')}
                else:
                    logger.error(f"AI响应失败 - 服务商: {provider_name}")
                    try:
                        increment_failure(provider_name)
                    except Exception:
                        pass
                    return {"success": False, "error": "AI响应失败"}
            except Exception as e:
                logger.error(f"同步AI响应异常: {str(e)}")
                try:
                    increment_failure(provider_name)
                except Exception:
                    pass
                return {"success": False, "error": f"同步AI响应异常: {str(e)}"}

        # 对于其他异步服务，使用事件循环
        import asyncio
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(service.generate_response(prompt, model_name, **kwargs))
                if result.get('success'):
                    logger.info(f"AI响应完成 - 服务商: {provider_name}, 输出长度: {len(result.get('content', ''))}")
                    # 成功时重置失败计数
                    try:
                        reset_failures(provider_name)
                    except Exception:
                        pass
                    return {"success": True, "response": result.get('content', '')}
                else:
                    logger.error(f"AI响应失败 - 服务商: {provider_name}")
                    try:
                        increment_failure(provider_name)
                    except Exception:
                        pass
                    return {"success": False, "error": "AI响应失败"}
            finally:
                # 确保正确关闭所有资源
                try:
                    if hasattr(service, 'client') and service.client and not service._client_closed:
                        loop.run_until_complete(service.client.aclose())
                        service._client_closed = True
                except Exception:
                    pass

                # 清理事件循环
                pending_tasks = asyncio.all_tasks(loop)
                if pending_tasks:
                    for task in pending_tasks:
                        task.cancel()
                    try:
                        loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
                    except Exception:
                        pass
                loop.close()
        except Exception as e:
            logger.error(f"同步AI响应异常: {str(e)}")
            try:
                increment_failure(provider_name)
            except Exception:
                pass
            return {"success": False, "error": f"同步AI响应异常: {str(e)}"}
    
    def calculate_tokens(self, provider_name: str, text: str) -> int:
        """计算Token数量"""
        service = self.get_service(provider_name)
        if not service:
            return 0
        
        return service.calculate_tokens(text)
    
    def estimate_cost(self, provider_name: str, input_tokens: int, output_tokens: int, 
                     model: str) -> float:
        """估算成本"""
        service = self.get_service(provider_name)
        if not service:
            return 0.0
        
        return service.estimate_cost(input_tokens, output_tokens, model)
    
    def reload_services(self):
        """重新加载所有AI服务"""
        self.services.clear()
        self.load_services()

# 全局AI服务管理器实例 - 延迟初始化
ai_manager = None

def get_ai_manager():
    """获取AI服务管理器实例（支持配置热重载）"""
    global ai_manager

    # 首次初始化
    if ai_manager is None:
        ai_manager = AIServiceManager()
        logger.info("AI服务管理器首次初始化完成")
        return ai_manager

    # 检查Redis配置版本是否变化
    try:
        _, redis_version = get_ai_config_from_redis()

        # 如果Redis中有新版本且与当前版本不同，重新加载服务
        if redis_version is not None and redis_version != ai_manager.config_version:
            logger.info(f"检测到AI配置版本变化：{ai_manager.config_version} -> {redis_version}，重新加载服务")
            ai_manager.reload_services()
            ai_manager.config_version = redis_version
        else:
            logger.debug(f"AI配置版本未变化，使用缓存的服务实例（版本: {ai_manager.config_version}）")

    except Exception as e:
        logger.warning(f"检查AI配置版本失败: {e}，使用当前服务实例")

    return ai_manager
