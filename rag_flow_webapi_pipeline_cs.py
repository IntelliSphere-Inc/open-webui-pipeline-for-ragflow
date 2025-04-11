"""
title: RagFlow Pipeline
author: luyilong2015
date: 2025-01-28
version: 1.0
license: MIT
description: A pipeline for retrieving relevant information from a knowledge base using the RagFlow's Chat Interface.
requirements: datasets>=2.6.1, sentence-transformers>=2.2.0
"""
#智能客服
from typing import List, Union, Generator, Iterator, Optional
from pydantic import BaseModel
import requests
import json

class Pipeline:
    class Valves(BaseModel):
        API_KEY: str
        AGENT_ID: str   # 这里 AGENT_ID 其实对应的是 'chat_id'
        HOST: str
        PORT: str

    def __init__(self):
        self.session_id = None
        self.debug = True
        self.sessionKV = {}
        self.valves = self.Valves(
            **{
                "API_KEY": "",
                "AGENT_ID": "",
                "HOST":"",
                "PORT":""
            }
        )

    async def on_startup(self):
        pass

    async def on_shutdown(self):
        pass

    async def inlet(self, body: dict, user: Optional[dict] = None) -> dict:
        print(f"inlet: {__name__}")
        if self.debug:
            chat_id = body['metadata']['chat_id']
            print(f"inlet: {__name__} - chat_id:{chat_id}")
            if self.sessionKV.get(chat_id):
                self.session_id = self.sessionKV.get(chat_id)
                print(f"cache ragflow's session_id is : {self.session_id}")
            else:
                # 创建 session (已改为 chats 路径)
                session_url = f"{self.valves.HOST}:{self.valves.PORT}/api/v1/chats/{self.valves.AGENT_ID}/sessions"
                session_headers = {
                    'content-Type': 'application/json',
                    'Authorization': 'Bearer ' + self.valves.API_KEY
                }
                session_data = {}
                session_response = requests.post(session_url, headers=session_headers, json=session_data)
                json_res = json.loads(session_response.text)
                self.session_id = json_res['data']['id']
                self.sessionKV[chat_id] = self.session_id
                print(f"new ragflow's session_id is : {json_res['data']['id']}")
            print(f"inlet: {__name__} - user:")
            print(user)
        return body

    async def outlet(self, body: dict, user: Optional[dict] = None) -> dict:
        print(f"outlet: {__name__}")
        if self.debug:
            print(f"outlet chat_id: {body['chat_id']}")
            print(f"outlet session_id: {body['session_id']}")
            print(f"outlet: {__name__} - user:")
            print(user)
        return body

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        # 同步改为 chats 路径
        question_url = f"{self.valves.HOST}:{self.valves.PORT}/api/v1/chats/{self.valves.AGENT_ID}/completions"
        question_headers = {
            'content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.valves.API_KEY
        }
        question_data = {
            'question': user_message,
            'stream': True,
            'session_id': self.session_id,
            'lang': 'Chinese'
        }
        question_response = requests.post(question_url, headers=question_headers, stream=True, json=question_data)

        if question_response.status_code == 200:
            step = 0
            for line in question_response.iter_lines():
                if line:
                    try:
                        json_data = json.loads(line.decode('utf-8')[5:])
                        # 确保有 'data' 且 data 不是 True，且有 'answer'
                        if 'data' in json_data and json_data['data'] is not True and 'answer' in json_data['data']:
                            ref_data = json_data['data'].get('reference', {})
                            # 仅当 reference.chunks 存在且不为空时，才 yield referenceStr
                            if 'chunks' in ref_data and ref_data['chunks']:
                                referenceStr = "\n\n### references\n\n"
                                filesList = []
                                for chunk in ref_data['chunks']:
                                    if chunk['document_id'] not in filesList:
                                        filename = chunk['document_name']
                                        parts = filename.split('.')
                                        ext = parts[-1].lower() if len(parts) > 1 else ''
                                        referenceStr += (
                                            f"\n\n - [{chunk['document_name']}]"
                                            f"({self.valves.HOST}:{self.valves.PORT}/document/{chunk['document_id']}"
                                            f"?ext={ext}&prefix=document)"
                                        )
                                        filesList.append(chunk['document_id'])
                                yield referenceStr
                            else:
                                # 原逻辑, 按增量方式输出回答
                                chunk_text = json_data['data']['answer'][step:]
                                yield chunk_text
                                step = len(json_data['data']['answer'])
                    except json.JSONDecodeError:
                        print(f"Failed to parse JSON: {line}")
        else:
            yield f"Workflow request failed with status code: {question_response.status_code}"
