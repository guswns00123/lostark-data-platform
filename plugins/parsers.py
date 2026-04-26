import re
import json

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
        
def parse_tooltip_content(tooltip_str, target_title):
    """Tooltip JSON에서 특정 타이틀 텍스트를 추출"""
    try:
        if not tooltip_str:
            return ""
        data = json.loads(tooltip_str)

        for key in data:
            element = data[key]
            if element and element.get("type") == "ItemPartBox":
                val = element.get("value", {})
                if target_title in val.get("Element_000", ""):
                    raw_text = val.get("Element_001", "")
                    clean = re.sub(r"<br\s*/?>", "\n", raw_text, flags=re.IGNORECASE)
                    clean = re.sub(r"<[^>]+>", "", clean)
                    return clean.strip()
        return ""
    except Exception as e:
        logger.warning(f"툴팁 파싱 에러: {e}")
        return ""

def split_core_options(core_opt_raw: str) -> dict:
    """
    ex) "[10P] 피해량 증가.\n[14P] 이동속도 증가." 
    => p1: 10, o1: "피해량 증가.", p2: 14, o2: "이동속도 증가." 형태로 파싱
    """
    # [숫자P] 와 다음 [숫자P] 사이의 모든 문자열(줄바꿈 포함)을 추출하는 정규식
    pattern = r'\[(\d+)P\]\s*(.*?)(?=\n\s*\[\d+P\]|$)'
    matches = re.findall(pattern, core_opt_raw, re.DOTALL)
    
    # 기본값은 None으로 채운 6단계 딕셔너리 생성
    parsed = {f"p{i}": None for i in range(1, 7)}
    parsed.update({f"o{i}": None for i in range(1, 7)})
    
    for idx, (pts, desc) in enumerate(matches):
        if idx >= 6:  # 로스트아크 아크 패시브 최대 레벨인 6을 초과하면 무시
            break
        parsed[f"p{idx+1}"] = int(pts)
        parsed[f"o{idx+1}"] = desc.strip()
        
    return parsed

def split_gem_effect(gem_eff_raw: str) -> dict:
    parsed = {
        "req_will": None, "will_eff": None,
        "pt_type": None, "pt_val": None,
        "e1_name": None, "e1_lvl": None, "e1_val": None,
        "e2_name": None, "e2_lvl": None, "e2_val": None,
    }
    if not gem_eff_raw:
        return parsed

    # 1. 의지력 파싱
    req_match = re.search(r"필요 의지력\s*:\s*(\d+)", gem_eff_raw)
    if req_match: 
        parsed["req_will"] = int(req_match.group(1))

    eff_match = re.search(r"의지력 효율\s*(\d+)", gem_eff_raw)
    if eff_match: 
        parsed["will_eff"] = int(eff_match.group(1))

    # 2. 포인트 파싱
    pt_match = re.search(r"(질서|도약|혼돈|깨달음|진화)\s*포인트\s*:\s*(\d+)", gem_eff_raw)
    if pt_match:
        parsed["pt_type"] = pt_match.group(1)
        parsed["pt_val"] = int(pt_match.group(2))

    # 3. 효과 파싱 및 💡숫자(소수점 포함)만 정교하게 추출
    eff_matches = re.findall(r"\[(.*?)\]\s*Lv\.(\d+)\s*\n(.*?)(?=\n\[|$)", gem_eff_raw, re.DOTALL)
    
    # 플러스/마이너스 기호 뒤의 숫자(소수점 포함)를 찾는 정규식
    num_pattern = r"[-+]?\s*(\d+\.?\d*)"
    
    if len(eff_matches) > 0:
        parsed["e1_name"] = eff_matches[0][0].strip()
        parsed["e1_lvl"] = int(eff_matches[0][1])
        
        # 💡 "공격력 +0.14%"에서 0.14만 찾아 float으로 변환
        val_match = re.search(num_pattern, eff_matches[0][2])
        parsed["e1_val"] = float(val_match.group(1)) if val_match else None
        
    if len(eff_matches) > 1:
        parsed["e2_name"] = eff_matches[1][0].strip()
        parsed["e2_lvl"] = int(eff_matches[1][1])
        
        # 💡 두 번째 효과도 동일하게 숫자만 추출
        val_match = re.search(num_pattern, eff_matches[1][2])
        parsed["e2_val"] = float(val_match.group(1)) if val_match else None

    return parsed

def strip_html(text):
    if not text:
        return ""
    # 1. <BR> 태그를 공백으로 치환하여 단어가 붙는 것을 방지
    text = re.sub(r"(?i)<br\s*/?>", " ", text)
    # 2. 모든 HTML 태그(<...>) 제거
    text = re.sub(r"<[^>]+>", "", text)
    # 3. &tdc_smart 같은 아이콘 렌더링 토큰 제거 (이게 핵심!)
    text = re.sub(r"&[a-zA-Z_]+\s*", "", text)
    return text.strip()

def parse_ark_passive_description(desc_html):
    """HTML이 포함된 효과 설명에서 티어, 특성명, 레벨을 추출"""
    if not desc_html:
        return None, None, None
    
    clean_text = strip_html(desc_html)
    # 예: '깨달음 1티어 고독한 기사 Lv.3'
    pattern = r"(\d+)티어\s+(.*?)\s+Lv\.(\d+)"
    match = re.search(pattern, clean_text)
    
    if match:
        # 💡 기존: f"{match.group(1)}티어" (문자열) 
        # 💡 변경: int(match.group(1)) (순수 숫자)
        tier = int(match.group(1)) 
        effect_name = match.group(2).strip()
        level = int(match.group(3))
        
        return tier, effect_name, level
        
    return None, None, None

def parse_rank_level(description: str):
    """ '6랭크 25레벨' 문자열에서 숫자만 추출하여 반환 (rank, level) """
    if not description:
        return None, None
    
    # 랭크와 레벨 앞의 숫자만 캡처하는 정규식
    match = re.search(r"(\d+)랭크\s*(\d+)레벨", description)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def parse_avatar_tooltip(tooltip_str):
    """
    툴팁 JSON에서 세분화된 기본 효과와 성향 스탯, 획득처를 추출합니다.
    """
    parsed = {
        "basic_stat": None, "basic_val": None,
        "intellect": 0, "courage": 0, "charm": 0, "kindness": 0,
        "source": None
    }
    
    if not tooltip_str:
        return parsed

    try:
        tooltip = json.loads(tooltip_str)
        
        for key, item in tooltip.items():
            if not isinstance(item, dict):
                continue

            item_type = item.get("type")
            item_value = item.get("value")

            if not item_value:
                continue

            # 1. 기본 효과 추출 및 분할 (예: "힘 +2.00%")
            if item_type == "ItemPartBox":
                el0 = item_value.get("Element_000", "")
                if "기본 효과" in el0:
                    basic_effect_str = strip_html(item_value.get("Element_001", ""))
                    # 정규식: (글자) +(숫자.숫자)%?
                    match = re.search(r"([가-힣]+)\s*\+?(\d+\.?\d*)", basic_effect_str)
                    if match:
                        parsed["basic_stat"] = match.group(1)
                        parsed["basic_val"] = float(match.group(2))

            # 2. 성향 스탯 추출 및 4개 컬럼으로 분할
            elif item_type == "SymbolString":
                title = item_value.get("titleStr", "")
                if "성향" in title:
                    tendency_str = strip_html(item_value.get("contentStr", ""))
                    
                    # 지성, 담력, 매력, 친절 각각의 숫자 추출
                    intel_m = re.search(r"지성\s*:\s*(\d+)", tendency_str)
                    if intel_m: parsed["intellect"] = int(intel_m.group(1))
                    
                    cour_m = re.search(r"담력\s*:\s*(\d+)", tendency_str)
                    if cour_m: parsed["courage"] = int(cour_m.group(1))
                    
                    charm_m = re.search(r"매력\s*:\s*(\d+)", tendency_str)
                    if charm_m: parsed["charm"] = int(charm_m.group(1))
                    
                    kind_m = re.search(r"친절\s*:\s*(\d+)", tendency_str)
                    if kind_m: parsed["kindness"] = int(kind_m.group(1))

            # 3. 획득처 추출
            elif item_type == "SingleTextBox":
                if "#5FD3F1" in item_value.upper():
                    parsed["source"] = strip_html(item_value)

        return parsed
    except Exception as e:
        logger.warning(f"아바타 툴팁 파싱 에러: {e}")
        return parsed

def extract_card_description(tooltip_str):
    """카드 툴팁에서 카드 세계관/배경 설명만 추출"""
    if not tooltip_str:
        return None
    try:
        tooltip = json.loads(tooltip_str)
        el_003 = tooltip.get("Element_003", {})
        if el_003.get("type") == "SingleTextBox":
            return strip_html(el_003.get("value", ""))
    except Exception:
        pass
    return None

def extract_basic_stats(effect_text):
    """
    '물리 방어력 +6863 마법 방어력 +7626 힘 +68588 체력 +8113' 형태의 문자열을 
    각 스탯별 정수형 값으로 파싱합니다.
    """
    # 기본값은 None (DB에 NULL로 들어가도록 처리)
    stats = {
        "무기 공격력": None,
        "물리 방어력": None,
        "마법 방어력": None,
        "힘": None,
        "민첩": None,
        "지능": None,
        "체력": None
    }
    
    if not effect_text:
        return stats

    # 정규식: (한글 및 띄어쓰기) +(숫자 및 쉼표)
    # 예: [('물리 방어력', '6863'), ('마법 방어력', '7626')]
    matches = re.findall(r"([가-힣\s]+)\s*\+([0-9,]+)", effect_text)
    
    for stat_name, stat_val in matches:
        clean_name = stat_name.strip()
        clean_val = int(stat_val.replace(",", "")) # 쉼표 제거 후 정수 변환
        
        if clean_name in stats:
            stats[clean_name] = clean_val
            
    return stats

def parse_additional_effect_to_json(item_type, text):
    if not text:
        return None

    result = {}
    clean_text = text.strip()

    # 1. 어빌리티 스톤: "[각인명] Lv.숫자" 형태
    if item_type == "어빌리티 스톤":
        # 예: "[아드레날린] Lv.2" -> {"아드레날린": 2}
        matches = re.findall(r"\[(.*?)\]\s*Lv\.(\d+)", clean_text)
        result["engravings"] = {name: int(level) for name, level in matches}

    # 2. 장신구(목/귀/반), 무기, 방어구: "스탯명 +수치(또는 %)" 형태
    elif item_type in ["목걸이", "귀걸이", "반지", "무기", "투구", "상의", "하의", "장갑", "어깨"]:
        # 예: "치명타 피해 +2.40%", "무기 공격력 +195"
        matches = re.findall(r"([가-힣\s]+)\s*\+([0-9.]+%?)", clean_text)
        for name, val in matches:
            clean_name = name.strip()
            # %(퍼센트) 옵션 vs +(정수) 옵션 구분 — 같은 스탯명의 % 옵션과 정수 옵션이 동시에 붙는 경우 키 충돌 방지
            suffix = "%" if val.endswith("%") else "+"
            result[f"{clean_name} {suffix}"] = val

    # 3. 팔찌: 기본 스탯(치/특/신 등) + 텍스트 효과 혼합 형태
    elif item_type == "팔찌":
        # 스탯 부분 추출 (예: "치명 +94", "힘 +10752")
        stat_matches = re.findall(r"(특화|치명|신속|제압|인내|숙련|힘|민첩|지능|체력)\s*\+([0-9]+)", clean_text)
        if stat_matches:
            result["stats"] = {name: int(val) for name, val in stat_matches}

        # 스탯 텍스트를 날려버리고 남은 "특수 부여 효과" 추출
        special_text = re.sub(r"(특화|치명|신속|제압|인내|숙련|힘|민첩|지능|체력)\s*\+[0-9]+\s*", "", clean_text).strip()
        if special_text:
            # parse_equipment_tooltip에서 각 부여 효과 시작 지점을 sentinel("LOSTARK_SPLIT_MARKER")로 표시해 둔 것을 split → 효과별 리스트
            effects = [e.strip() for e in special_text.split("LOSTARK_SPLIT_MARKER") if e.strip()]
            if effects:
                result["special_effects"] = effects

    # 4. 내실 아이템 (보주, 부적, 나침반): 텍스트가 복잡하므로 원문 또는 특정 키워드로 저장
    elif item_type in ["보주", "부적", "나침반"]:
        result["description"] = clean_text

    # 파이썬 딕셔너리를 JSON 문자열로 변환하여 리턴 (Postgres JSONB에 호환됨)
    return json.dumps(result, ensure_ascii=False)

def parse_basic_effect_to_json(text):
    if not text:
        return None
        
    stats = {}
    # 정규식: "물리 방어력 +6863" -> ("물리 방어력", "6863")
    matches = re.findall(r"([가-힣\s]+)\s*\+([0-9,]+)", text)
    
    for stat_name, stat_val in matches:
        clean_name = stat_name.strip()
        # 쉼표 제거 후 정수로 변환
        clean_val = int(stat_val.replace(",", ""))
        stats[clean_name] = clean_val
        
    # 결과가 없으면 원문 텍스트라도 description 형태로 보존
    if not stats:
        return json.dumps({"description": text.strip()}, ensure_ascii=False)
        
    return json.dumps(stats, ensure_ascii=False)

def parse_equipment_tooltip(tooltip_str, item_name):
    quality, item_tier = 0, None
    basic_effect, additional_effect, ark_passive_effect = None, None, None
    enhancement_level, advanced_reinforcement = 0, 0

    # 1. 아이템 이름에서 강화 수치 추출 (+14)
    match = re.search(r"\+(\d+)", item_name)
    if match:
        enhancement_level = int(match.group(1))

    if not tooltip_str:
        return (enhancement_level, quality, item_tier, basic_effect, 
                additional_effect, ark_passive_effect, advanced_reinforcement)

    try:
        tooltip = json.loads(tooltip_str)
        for key, item in tooltip.items():
            if not isinstance(item, dict):
                continue
                
            i_type = item.get("type")
            i_val = item.get("value")

            # 2. 상급 재련 단계 추출
            if i_type == "SingleTextBox":
                clean_text = strip_html(str(i_val))
                if "[상급 재련]" in clean_text:
                    adv_match = re.search(r"(\d+)(?=단계)", clean_text)
                    if adv_match:
                        advanced_reinforcement = int(adv_match.group(1))
                    else:
                        nums = re.findall(r"\d+", clean_text)
                        if nums:
                            advanced_reinforcement = int(nums[0])

            # 3. 품질 및 티어 추출
            elif i_type == "ItemTitle":
                quality = i_val.get("qualityValue", 0)
                t_match = re.search(r"티어\s*(\d+)", strip_html(i_val.get("leftStr2", "")))
                if t_match:
                    item_tier = int(t_match.group(1))

            # 4. 각종 효과 텍스트 추출
            elif i_type == "ItemPartBox":
                el0_raw = i_val.get("Element_000", "")
                el1_raw = i_val.get("Element_001", "")
                
                el0 = strip_html(el0_raw)
                el1 = strip_html(el1_raw)
                
                if "기본 효과" in el0:
                    basic_effect = el1
                elif "팔찌 효과" in el0:
                    # 팔찌 효과는 각 부여 효과 앞에 <img src='emoticon_tooltip_bracelet_(locked|changeable)'> 아이콘이 붙는 구조.
                    # 이 아이콘 위치를 sentinel("LOSTARK_SPLIT_MARKER")로 치환해 효과 경계 보존.
                    # <BR>은 같은 효과 내 줄바꿈일 수 있으므로 공백으로 처리(over-split 방지).
                    text_with_sep = re.sub(r"<img[^>]*emoticon_tooltip_bracelet_[^>]*>", "LOSTARK_SPLIT_MARKER", str(el1_raw))
                    text_with_sep = re.sub(r"(?i)<br\s*/?>", " ", text_with_sep)
                    text_with_sep = re.sub(r"<[^>]+>", "", text_with_sep)
                    text_with_sep = re.sub(r"&[a-zA-Z_]+\s*", "", text_with_sep).strip()
                    additional_effect = text_with_sep if not additional_effect else f"{additional_effect} | {text_with_sep}"
                elif any(keyword in el0 for keyword in ["추가 효과", "연마 효과"]):
                    additional_effect = el1 if not additional_effect else f"{additional_effect} | {el1}"
                elif "아크 패시브" in el0:
                    ark_passive_effect = el1
                
                # 💡 [추가] 보주 아이템의 "특수 효과" 처리
                elif "특수 효과" in el0:
                    # 원문에서 <BR>이나 <br>을 기준으로 문장을 쪼갠 뒤 HTML 태그 제거
                    parts = [strip_html(p) for p in re.split(r'<BR>|<br>', str(el1_raw), flags=re.IGNORECASE)]
                    
                    for part in parts:
                        if not part: continue
                        
                        # 낙원력 관련 문구는 추가 효과(additional_effect)로 배치
                        if "최대 낙원력 :" in part:
                            additional_effect = part if not additional_effect else f"{additional_effect} | {part}"
                        # "수치가 변동됩니다" 같은 단순 안내 문구는 스킵
                        elif "수치가 변동됩니다" in part:
                            continue
                        # 메인 스킬 효과(흰 꽃의 화원 등)는 기본 효과(basic_effect)로 배치
                        else:
                            basic_effect = part if not basic_effect else f"{basic_effect} | {part}"

            # 5. 어빌리티 스톤 각인 효과 추출
            elif i_type == "IndentStringGroup":
                content_obj = i_val.get("Element_000", {}).get("contentStr", {})
                stone_effects = [strip_html(v.get("contentStr", "")) for k, v in content_obj.items() if strip_html(v.get("contentStr", ""))]
                if stone_effects:
                    stone_str = ", ".join(stone_effects)
                    additional_effect = stone_str if not additional_effect else f"{additional_effect} | {stone_str}"

    except Exception as e:
        logger.warning(f"장비 파싱 중 에러 ({item_name}): {e}")

    parsed_stats = extract_basic_stats(basic_effect)

    return (
        enhancement_level, 
        quality, 
        item_tier, 
        basic_effect,          # 👈 날것 그대로의 문자열 (raw)
        additional_effect,     # 👈 날것 그대로의 문자열 (raw)
        ark_passive_effect, 
        advanced_reinforcement
    )

def parse_gem_effects(eff_type_str, eff_opt_str):
    """
    보석의 효과 텍스트에서 명칭과 수치를 분리합니다.
    """
    eff_name = None
    eff_val = None
    basic_atk_val = None

    # 1. effect_type 파싱 (예: "피해 40.00% 증가")
    if eff_type_str:
        match = re.search(r"(.+?)\s+([\d\.]+)\%\s+(.+)", eff_type_str)
        if match:
            eff_name = f"{match.group(1).strip()} {match.group(3).strip()}" # "피해 증가"
            eff_val = float(match.group(2)) # 40.00
        else:
            eff_name = eff_type_str

    # 2. effect_option 파싱 (예: "기본 공격력 1.00% 증가")
    if eff_opt_str:
        match = re.search(r"([\d\.]+)", eff_opt_str)
        if match:
            basic_atk_val = float(match.group(1)) # 1.00

    return eff_name, eff_val, basic_atk_val

def clean_number(val):
    if val is None or val == "":
        return 0.0
    try:
        return float(str(val).replace(",", ""))
    except ValueError:
        return 0.0

def parse_skill_tooltip(tooltip_str):
    """스킬 툴팁에서 쿨타임, 마나, 기믹, 스킬 설명, 룬 효과를 추출합니다."""
    res = {
        "cooldown": None, "mana_cost": 0, "weak_point": 0,
        "stagger": None, "attack_type": None, "is_counter": False,
        "skill_description": None, "rune_effect": None
    }

    if not tooltip_str:
        return res

    try:
        tooltip = json.loads(tooltip_str)
    except Exception:
        return res

    for key, item in tooltip.items():
        if not isinstance(item, dict):
            continue

        i_type = item.get("type")
        i_val = item.get("value")

        # 1. 쿨타임
        if i_type == "CommonSkillTitle":
            cd_match = re.search(r"재사용 대기시간\s*([\d\.]+)초", strip_html(i_val.get("leftText", "")))
            if cd_match: res["cooldown"] = float(cd_match.group(1))

        # 2. 마나
        elif i_type in ("MultiTextBox", "SingleTextBox"):
            mana_match = re.search(r"마나\s*(\d+)\s*소모", strip_html(str(i_val)))
            if mana_match: res["mana_cost"] = int(mana_match.group(1))

        # 3. 전투 기믹 및 💡스킬 기본 설명
        if i_type == "SingleTextBox":
            val_text_raw = str(i_val)
            val_text_clean = strip_html(val_text_raw)

            # 기믹 파싱
            wp_match = re.search(r"부위 파괴\s*:\s*레벨\s*(\d+)", val_text_clean)
            if wp_match: res["weak_point"] = int(wp_match.group(1))
            
            st_match = re.search(r"무력화\s*:\s*([가-힣]+)", val_text_clean)
            if st_match: res["stagger"] = st_match.group(1)
            
            at_match = re.search(r"공격 타입\s*:\s*([가-힣\s]+)", val_text_clean)
            if at_match: res["attack_type"] = at_match.group(1).strip()
            
            if "카운터 : 가능" in val_text_clean: res["is_counter"] = True

            # 툴팁 구조상 <BR> 이전에 스킬 설명이 위치함
            if "<BR>" in val_text_raw.upper() and res["skill_description"] is None:
                # 기믹 텍스트가 섞여있다면 스킬 설명일 확률이 99%
                if "부위 파괴" in val_text_clean or "무력화" in val_text_clean or "공격 타입" in val_text_clean:
                    parts = re.split(r"(?i)<br\s*/?>", val_text_raw)
                    if parts:
                        res["skill_description"] = strip_html(parts[0])

        # 4. 💡 룬 효과 파싱
        if i_type == "ItemPartBox":
            el0 = i_val.get("Element_000", "")
            if "스킬 룬 효과" in el0:
                res["rune_effect"] = strip_html(i_val.get("Element_001", ""))

    return res

def to_jsonb(val):
    return json.dumps(val, ensure_ascii=False) if val else None

