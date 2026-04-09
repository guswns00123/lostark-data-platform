"""
로스트아크 전투 각인 데이터 크롤링 및 DB 적재.

Selenium을 사용하여 동적 렌더링된 각인 상세 페이지를 파싱합니다.

적재 테이블:
  - lostark.engrave
"""

import json
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import HEADERS
from db import load_dataframe


def extract_engrave_details() -> pd.DataFrame:
    """
    전체 각인 데이터를 Selenium으로 크롤링합니다.

    Returns:
        각인 데이터 DataFrame
    """
    # 1단계: 각인 코드 목록 확보
    print("[1/3] 전투 각인 코드 목록 수집 중...")
    list_url = "https://lostark.inven.co.kr/dataninfo/engrave/"

    res = requests.get(list_url, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")
    main_tag = soup.find("main", id="lostarkDbEngrave")

    if not main_tag:
        print("  메인 JSON 데이터를 찾을 수 없습니다.")
        return pd.DataFrame()

    data = json.loads(main_tag["data-base-info"])
    engrave_list = data["contentsInfo"]["listData"]
    total = len(engrave_list)
    print(f"  총 {total}개의 각인 코드 확보 완료")

    # 2단계: Selenium으로 상세 파싱
    print(f"[2/3] Selenium 상세 파싱 시작...")

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=chrome_options)
    parsed_data = []

    try:
        for i, item in enumerate(engrave_list, start=1):
            code = str(item.get("code", ""))
            name = item.get("name", "")
            search_name = item.get("search_name", "").replace("\n", ", ")

            detail_url = f"https://lostark.inven.co.kr/dataninfo/engrave?code={code}"
            print(f"  [{i}/{total}] '{name}' 추출 중...")

            driver.get(detail_url)

            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.engrave"))
                )
            except Exception:
                print(f"    렌더링 시간 초과 ({name})")
                continue

            d_soup = BeautifulSoup(driver.page_source, "html.parser")

            row_dict = {
                "engrave_code": code,
                "engrave_name": name,
                "search_name": search_name,
                "icon_url": "-",
                "legend_final": "-",
                "relic_final": "-",
                "basic_effect": "-",
                "legend_effect": "-",
                "relic_effect": "-",
                "stone_effect": "-",
            }

            container = d_soup.find("div", class_=re.compile(r"engrave"))
            if container:
                icon_tag = container.select_one("div.img img")
                if icon_tag:
                    row_dict["icon_url"] = icon_tag.get("src", "-")

                sections = container.find_all("section")
                for sec in sections:
                    h4 = sec.find("h4")
                    if not h4:
                        continue
                    title = h4.text.strip()

                    if "최종 적용 효과" in title:
                        p_text = sec.find("p").get_text(separator=" ", strip=True) if sec.find("p") else "-"
                        p_text = re.sub(r"\s+", " ", p_text)
                        if "전설" in title:
                            row_dict["legend_final"] = p_text
                        elif "유물" in title:
                            row_dict["relic_final"] = p_text

                    elif "기본 효과 및 등급별 효과" in title:
                        effect_wrap = sec.find("div", class_="effect-wrap")
                        if effect_wrap:
                            for p_tag in effect_wrap.find_all("p"):
                                text = re.sub(r"\s+", " ", p_tag.get_text(separator=" ", strip=True))
                                if "기본 -" in text:
                                    row_dict["basic_effect"] = text
                                elif "전설 -" in text:
                                    row_dict["legend_effect"] = text
                                elif "유물 -" in text:
                                    row_dict["relic_effect"] = text

                    elif "어빌리티 스톤" in title:
                        p_text = sec.find("p").get_text(separator=" ", strip=True) if sec.find("p") else "-"
                        row_dict["stone_effect"] = re.sub(r"\s+", " ", p_text)

            parsed_data.append(row_dict)
            time.sleep(0.3)

    finally:
        driver.quit()

    df = pd.DataFrame(parsed_data)
    print(f"  총 {len(df)}개 각인 수집 완료")
    return df


def run():
    """각인 데이터를 크롤링하고 DB에 적재합니다."""
    df = extract_engrave_details()
    if not df.empty:
        print("[3/3] DB 적재 중...")
        load_dataframe(df, "engrave", truncate=True)


if __name__ == "__main__":
    run()
