"""
로스트아크 룬 데이터 크롤링 및 DB 적재.

적재 테이블:
  - lostark.lostark_rune_tb
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import HEADERS
from db import load_dataframe

URL = "https://lostark.inven.co.kr/dataninfo/item/?datagroup=etc&itemclass3=33100"
GRADE_MAP = {"5": "전설", "4": "영웅", "3": "희귀", "2": "고급", "1": "일반"}


def extract_runes() -> pd.DataFrame:
    """
    인벤에서 전체 룬 데이터를 크롤링합니다.

    Returns:
        룬 데이터 DataFrame
    """
    print("룬 데이터 수집 시작...")
    response = requests.get(URL, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    target_table = soup.find("table", class_="list_table")

    if not target_table:
        print("  테이블을 찾을 수 없습니다.")
        return pd.DataFrame()

    rows = target_table.select("tbody tr[data-item-grade]")
    data_list = []

    for row in rows:
        grade = GRADE_MAP.get(row.get("data-item-grade", ""), "알수없음")
        tds = row.find_all("td")
        if len(tds) < 4:
            continue

        img_tag = tds[0].find("img")
        icon_url = "https:" + img_tag["src"] if img_tag and img_tag.has_attr("src") else ""

        name_tag = tds[1].find("a", class_="name")
        name = name_tag.get_text(strip=True).split("[")[0].strip() if name_tag else ""

        level = tds[2].get_text(strip=True)

        p_tags = tds[3].find_all("p")
        effect, buy_condition = "", ""
        for p in p_tags:
            classes = p.get("class", [])
            if "layersubtitle" in classes or "itemdesc" in classes:
                continue
            elif "buycondition" in classes:
                buy_condition = p.get_text(strip=True).replace("구매조건 :", "").strip()
            else:
                effect += p.get_text(strip=True) + " "

        data_list.append({
            "grade": grade,
            "rune_name": name,
            "required_level": level,
            "rune_effect": effect.strip(),
            "buy_condition": buy_condition,
            "icon_url": icon_url,
        })

    df = pd.DataFrame(data_list)
    print(f"  {len(df)}개 룬 수집 완료")
    return df


def run():
    """룬 데이터를 크롤링하고 DB에 적재합니다."""
    df = extract_runes()
    if not df.empty:
        load_dataframe(df, "lostark_rune_tb")


if __name__ == "__main__":
    run()
