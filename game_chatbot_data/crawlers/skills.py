"""
로스트아크 스킬 데이터 크롤링 및 DB 적재.

수집 대상: 인벤 스킬 상세 페이지
적재 테이블:
  - lostark.lostark_skill_level   (레벨별 스탯)
  - lostark.lostark_skill_summary (스킬 요약)
  - lostark.lostark_skill_tripod  (트라이포드)
"""

import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import HEADERS, JOB_CODES
from db import load_dataframe

BASE_URL = "https://lostark.inven.co.kr/dataninfo/skill/"


def extract_skills(job_code: int, limit: int | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    특정 직업의 전체 스킬 데이터를 크롤링합니다.

    Args:
        job_code: 직업 코드 (예: 102=디스트로이어)
        limit: 수집할 스킬 수 제한 (None이면 전체)

    Returns:
        (레벨 테이블 DF, 요약 DF, 트라이포드 DF)
    """
    list_url = f"{BASE_URL}?reqjob={job_code}"

    print(f"[1/3] 직업코드 {job_code} 스킬 리스트 확보 중...")
    res = requests.get(list_url, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")

    skill_tags = soup.find_all("a", attrs={"data-lostark-skill-code": True})
    skill_dict = {
        tag.get("data-lostark-skill-code"): tag.find("p").text.strip()
        for tag in skill_tags
        if tag.find("p")
    }

    level_data, summary_data, tripod_data = [], [], []
    target_list = list(skill_dict.items())[:limit] if limit else list(skill_dict.items())

    print(f"[2/3] 총 {len(target_list)}개 스킬 상세 파싱 시작...")

    for i, (code, name) in enumerate(target_list):
        detail_url = f"{BASE_URL}?code={code}"
        print(f"  [{i+1}/{len(target_list)}] '{name}' 수집 중...")

        try:
            d_res = requests.get(detail_url, headers=HEADERS)
            d_soup = BeautifulSoup(d_res.text, "html.parser")

            # 스킬 아이콘
            s_icon = d_soup.select_one(".lostark.tooltip_top .icon_wrap img")
            s_icon_url = f"https:{s_icon['src']}" if s_icon and "src" in s_icon.attrs else "-"

            # 1. 레벨별 스탯
            s_table = d_soup.select_one("table.skill_table tbody")
            if s_table:
                for row in s_table.find_all("tr"):
                    th, td = row.find("th"), row.find("td")
                    if th and td:
                        lvl = th.text.strip()
                        p_val = td.find("p", class_="value")
                        sp, res_cost = "-", "-"
                        if p_val:
                            l_span = p_val.find("span", class_="left")
                            r_span = p_val.find("span", class_="right")
                            if l_span and l_span.find("b"):
                                sp = l_span.find("b").text.strip()
                            if r_span and r_span.find("b"):
                                res_cost = r_span.find("b").text.strip()
                            p_val.decompose()
                        desc = re.sub(r"\s+", " ", td.text).strip()
                        level_data.append({
                            "skill_code": code,
                            "skill_name": name,
                            "skill_icon_url": s_icon_url,
                            "skill_level": lvl,
                            "req_points": sp,
                            "resource_cost": res_cost,
                            "description": desc,
                        })

            # 2. 스킬 요약
            d_list = d_soup.select_one("ul.data_list")
            if d_list:
                for li in d_list.find_all("li", class_="lostark"):
                    tag = li.find("span", class_="tag")
                    if tag:
                        it_name = tag.text.strip()
                        tag.decompose()
                        summary_data.append({
                            "skill_code": code,
                            "skill_name": name,
                            "skill_icon_url": s_icon_url,
                            "item_name": it_name,
                            "item_content": re.sub(r"\s+", " ", li.text).strip(),
                        })

            # 3. 트라이포드
            t_area = d_soup.select_one(".skill_tripod.right")
            if t_area:
                for block in t_area.find_all("div", class_="tripod"):
                    t_tier_tag = block.find("p", class_="tripod_tier")
                    t_tier = t_tier_tag.text.strip() if t_tier_tag else "-"
                    for li in block.find_all("li", class_="lostark"):
                        tn = li.find("span", class_="name")
                        td_tag = li.find("p", class_="tripod_desc")
                        ti = li.select_one(".icon_wrap img")
                        ti_url = f"https:{ti['src']}" if ti and "src" in ti.attrs else "-"
                        if tn and td_tag:
                            tripod_data.append({
                                "skill_code": code,
                                "skill_name": name,
                                "skill_icon_url": s_icon_url,
                                "tripod_icon_url": ti_url,
                                "unlock_tier": t_tier,
                                "tripod_name": tn.text.strip(),
                                "description": td_tag.text.strip(),
                            })

            time.sleep(0.3)

        except Exception as e:
            print(f"  {name} 파싱 중 오류: {e}")
            continue

    return pd.DataFrame(level_data), pd.DataFrame(summary_data), pd.DataFrame(tripod_data)


def run(job_codes: list[int] | None = None):
    """전체 직업의 스킬 데이터를 크롤링하고 DB에 적재합니다."""
    codes = job_codes or JOB_CODES
    for job_code in codes:
        print(f"\n{'='*50}")
        print(f"직업코드 {job_code} 처리 시작")
        print(f"{'='*50}")

        df_lvl, df_sum, df_tri = extract_skills(job_code=job_code)

        if not df_lvl.empty:
            print("[3/3] DB 적재 중...")
            load_dataframe(df_lvl, "lostark_skill_level")
            load_dataframe(df_sum, "lostark_skill_summary")
            load_dataframe(df_tri, "lostark_skill_tripod")


if __name__ == "__main__":
    run()
