# dags/utils/extractors.py
import requests
import urllib.parse
import logging
from config import BASE_URL
import time
import json
logger = logging.getLogger(__name__)

def fetch_sibling_characters(character_name: str, api_key: str):
    """
    특정 캐릭터의 원정대(siblings) 캐릭터 목록을 조회합니다.
    """
    encoded_name = urllib.parse.quote(character_name)
    url = f"{BASE_URL}/characters/{encoded_name}/siblings"

    headers = {
        "accept": "application/json",
        "authorization": f"bearer {api_key}"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"❌ siblings API 호출 실패 ({response.status_code}): {response.text}")
            return []
    except Exception as e:
        logger.error(f"❌ siblings API 통신 중 에러 발생: {e}")
        return []


def fetch_armory_data(character_name: str, api_key: str):
    """
    로스트아크 API를 호출하여 캐릭터 종합 정보를 가져옵니다.
    """
    encoded_name = urllib.parse.quote(character_name)
    url = f"{BASE_URL}/armories/characters/{encoded_name}"
    
    headers = {
        "accept": "application/json", 
        "authorization": f"bearer {api_key}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"❌ API 호출 실패 ({response.status_code}): {response.text}")
            return None
    except Exception as e:
        logger.error(f"❌ API 통신 중 에러 발생: {e}")
        return None

def fetch_sibling_characters(character_name: str, api_key: str):
    """
    특정 캐릭터의 원정대(siblings) 캐릭터 목록을 조회합니다.
    """
    encoded_name = urllib.parse.quote(character_name)
    url = f"{BASE_URL}/characters/{encoded_name}/siblings"

    headers = {
        "accept": "application/json",
        "authorization": f"bearer {api_key}"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"❌ siblings API 호출 실패 ({response.status_code}): {response.text}")
            return []
    except Exception as e:
        logger.error(f"❌ siblings API 통신 중 에러 발생: {e}")
        return []


def fetch_market_data(search_payload: dict, api_key: str):
    """
    거래소 API를 호출하여 검색 조건에 맞는 모든 아이템 리스트를 반환합니다.
    """
    url = f"{BASE_URL}/markets/items"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"bearer {api_key}"
    }
    
    all_items = []
    page_no = 1

    while True:
        payload = search_payload.copy()
        payload["PageNo"] = page_no
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code == 429:
                logger.warning("⚠️ Rate Limit! 60초 대기...")
                time.sleep(60)
                continue
                
            if response.status_code != 200:
                logger.error(f"❌ API 실패 ({response.status_code})")
                break

            data = response.json()
            items = data.get("Items", [])
            
            if not items:
                break

            all_items.extend(items)
            page_no += 1
            time.sleep(0.5) # 부하 방지
            
        except Exception as e:
            logger.error(f"❌ API 통신 중 에러: {e}")
            break
            
    return all_items

def fetch_auction_data(search_payload: dict, api_key: str):
    """
    경매장 API를 호출하여 검색 조건에 맞는 모든 매물 리스트를 반환합니다.
    """
    url = "https://developer-lostark.game.onstove.com/auctions/items"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"bearer {api_key}"
    }
    
    all_items = []
    page_no = 1

    while True:
        payload = search_payload.copy()
        payload["PageNo"] = page_no
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code == 429:
                logger.warning("⚠️ Rate Limit! 60초 대기...")
                time.sleep(60)
                continue
                
            if response.status_code != 200:
                logger.error(f"❌ API 실패 ({response.status_code})")
                break

            data = response.json()
            items = data.get("Items", [])
            
            if not items:
                break

            all_items.extend(items)
            page_no += 1
            time.sleep(0.5) # 부하 방지
            
        except Exception as e:
            logger.error(f"❌ API 통신 중 에러: {e}")
            break
            
    return all_items
