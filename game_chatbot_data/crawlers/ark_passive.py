"""
로스트아크 아크 패시브 데이터 크롤링 및 DB 적재.

기존 셀 3(코어 37700), 셀 4(패시브), 셀 7(코어 37701)을 통합.
item_class 파라미터로 37700/37701 분기, 패시브 효과는 별도 함수.

적재 테이블:
  - lostark.ark_passive_core  (코어 37700)
  - lostark.ark_grid_core     (코어 37701)
  - lostark.ark_passive       (패시브 효과)
"""

import json
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import HEADERS, JOB_CODES
from db import load_dataframe


# ──────────────────────────────────────────────
# 1. 아크 패시브 코어 (37700 / 37701)
# ──────────────────────────────────────────────

def extract_cores(job_code: int | None = None, item_class: int = 37700) -> pd.DataFrame:
    """
    아크 패시브 코어 데이터를 크롤링합니다.

    Args:
        job_code: 직업 코드 (37701은 직업 코드 없이 단일 URL)
        item_class: 37700(일반 코어) 또는 37701(그리드 코어)

    Returns:
        정제된 코어 DataFrame
    """
    if item_class == 37701:
        url = f"https://lostark.inven.co.kr/dataninfo/item/?datagroup=etc&itemclass3={item_class}"
    else:
        url = f"https://lostark.inven.co.kr/dataninfo/item/?datagroup=etc&itemclass3={item_class}&reqjob={job_code}"

    print(f"  코어({item_class}) 데이터 수집 중... (job_code={job_code})")

    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    tbody = soup.select_one(".db_board.db_item table tbody")
    if not tbody:
        print("  데이터를 찾을 수 없습니다.")
        return pd.DataFrame()

    parsed_data = []

    for row in tbody.find_all("tr"):
        if "noResult" in row.get("class", []):
            continue
        tds = row.find_all("td")
        if len(tds) < 4:
            continue

        a_tag = tds[0].find("a")
        item_code = a_tag["data-lostark-item-code"] if a_tag and "data-lostark-item-code" in a_tag.attrs else "-"
        img_tag = tds[0].find("img")
        icon_url = f"https:{img_tag['src']}" if img_tag and "src" in img_tag.attrs else "-"
        name_tag = tds[1].find("a", class_="name")
        item_name = name_tag.text.strip() if name_tag else tds[1].text.strip()

        row_dict = {
            "item_code": item_code,
            "item_name": item_name,
            "icon_url": icon_url,
            "req_level": tds[2].text.strip(),
        }

        desc_td = tds[3]
        subtitles = desc_td.find_all("p", class_="layersubtitle")

        for sub in subtitles:
            key = sub.text.strip()
            desc_p = sub.find_next_sibling("p", class_="itemdesc")
            if not desc_p:
                continue

            for br in desc_p.find_all("br"):
                br.replace_with("\n")
            val = re.sub(r" +", " ", desc_p.get_text(separator=" ").strip())

            if key == "코어 옵션":
                options = re.findall(r"\[(\d+P)\](.*?)(?=\[\d+P\]|$)", val, flags=re.DOTALL)
                for pt, desc in options:
                    row_dict[f"option_{pt.lower()}"] = desc.strip().replace("\n", " ")
            elif key == "코어 타입":
                row_dict["core_type"] = val.replace("\n", " / ")
            elif key == "코어 공급 의지력":
                row_dict["core_energy"] = val.replace("\n", " / ")
            elif key == "코어 옵션 발동 조건":
                if item_class == 37701:
                    # 37701 전용: 전용 패시브명 / 4티어 패시브명 분리
                    val_clean = val.replace("\n", "/")
                    parts = val_clean.split("/")
                    row_dict["전용_ark_passive_name"] = parts[0].replace("전용", "").strip() if parts else "-"
                    row_dict["4_tier_ark_passive_name"] = (
                        parts[1].replace("아크 패시브 4티어", "").replace("활성화 필요", "").strip()
                        if len(parts) >= 2 else "-"
                    )
                else:
                    row_dict["activation_condition"] = val.replace("\n", " / ")

        parsed_data.append(row_dict)

    df = pd.DataFrame(parsed_data)

    # 스키마에 맞는 컬럼 보장
    if item_class == 37701:
        expected = [
            "item_code", "item_name", "icon_url", "req_level",
            "core_type", "core_energy",
            "option_10p", "option_14p", "option_17p", "option_18p", "option_19p", "option_20p",
            "전용_ark_passive_name", "4_tier_ark_passive_name",
        ]
    else:
        expected = [
            "item_code", "item_name", "icon_url", "req_level",
            "core_type", "core_energy", "activation_condition",
            "option_10p", "option_14p", "option_17p", "option_18p", "option_19p", "option_20p",
        ]

    for col in expected:
        if col not in df.columns:
            df[col] = "-"
    df = df.fillna("-")[expected]

    print(f"  {len(df)}개 코어 수집 완료")
    return df


# ──────────────────────────────────────────────
# 2. 아크 패시브 효과
# ──────────────────────────────────────────────

def extract_passive_effects(job_code: int) -> pd.DataFrame:
    """
    특정 직업의 아크 패시브 효과 데이터를 크롤링합니다.

    Args:
        job_code: 직업 코드

    Returns:
        정제된 패시브 효과 DataFrame
    """
    url = f"https://lostark.inven.co.kr/dataninfo/arkpassive/?code={job_code}"

    print(f"  아크 패시브 효과 수집 중... (job_code={job_code})")

    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        main_tag = soup.find("main", id="lostarkDbArkPassive")
        if not main_tag or not main_tag.has_attr("data-base-info"):
            print("  'data-base-info' 속성을 찾을 수 없습니다.")
            return pd.DataFrame()

        data = json.loads(main_tag["data-base-info"])
        job_name = data["contentsInfo"]["job"]["name"]
        pages = data["contentsInfo"]["passivePage"]

        stat_passives = ["치명", "특화", "제압", "신속", "인내", "숙련"]
        parsed_list = []

        for page_key, page_info in pages.items():
            category_name = page_info.get("name", "")
            for tier_data in page_info.get("tierList", []):
                tier_num = tier_data.get("tier", "")
                for passive in tier_data.get("passiveList", []):
                    name = passive.get("name", "")
                    row_dict = {
                        "job_name": job_name,
                        "category": category_name,
                        "tier": f"{tier_num}티어",
                        "passive_code": str(passive.get("code", "")),
                        "passive_name": name,
                        "icon_url": f"https://static.inven.co.kr/image_2011/site_image/lostark/arkpassiveicon/{passive.get('icon', '')}.png",
                        "req_points": str(passive.get("point", "")),
                        "max_level": str(passive.get("max_level", "")),
                    }

                    # 스탯 패시브는 lv10/20/30만 수집 (나머지 레벨은 값이 선형이라 불필요)
                    raw_descs = passive.get("desc", [])
                    for i, d in enumerate(raw_descs, start=1):
                        if name in stat_passives and i not in [10, 20, 30]:
                            continue
                        d_replaced = d.replace("<br>", "\n").replace("&nbsp;", " ")
                        clean_text = BeautifulSoup(d_replaced, "html.parser").get_text().strip()
                        row_dict[f"lv{i}_effect"] = clean_text

                    parsed_list.append(row_dict)

        df = pd.DataFrame(parsed_list)

        expected = [
            "job_name", "category", "tier", "passive_code", "passive_name",
            "icon_url", "req_points", "max_level",
            "lv1_effect", "lv2_effect", "lv3_effect", "lv4_effect", "lv5_effect",
            "lv10_effect", "lv20_effect", "lv30_effect",
        ]
        for col in expected:
            if col not in df.columns:
                df[col] = "-"
        df = df.fillna("-")[expected]

        print(f"  {len(df)}개 패시브 수집 완료")
        return df

    except Exception as e:
        print(f"  에러 발생: {e}")
        return pd.DataFrame()


# ──────────────────────────────────────────────
# 실행 진입점
# ──────────────────────────────────────────────

def run(job_codes: list[int] | None = None):
    """전체 직업의 아크 패시브 데이터를 크롤링하고 DB에 적재합니다."""
    codes = job_codes or JOB_CODES

    # 1) 코어 37700 (직업별)
    print("\n[코어 37700] 직업별 수집 시작")
    for job_code in codes:
        df = extract_cores(job_code=job_code, item_class=37700)
        if not df.empty:
            load_dataframe(df, "ark_passive_core")

    # 2) 코어 37701 (직업 무관, 단일 URL)
    print("\n[코어 37701] 수집 시작")
    df_grid = extract_cores(item_class=37701)
    if not df_grid.empty:
        load_dataframe(df_grid, "ark_grid_core")

    # 3) 패시브 효과 (직업별)
    print("\n[패시브 효과] 직업별 수집 시작")
    for job_code in codes:
        df = extract_passive_effects(job_code=job_code)
        if not df.empty:
            load_dataframe(df, "ark_passive")


if __name__ == "__main__":
    run()
