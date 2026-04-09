"""
skills 크롤러 유닛 테스트.

실제 HTTP 요청 없이 BeautifulSoup 파싱 로직을 검증합니다.
unittest.mock으로 requests.get 호출을 가짜 응답으로 대체합니다.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# 테스트용 최소 HTML 픽스처
MOCK_SKILL_HTML = """
<html><body>
<div class="lostark-db-skills-wrap">
  <ul>
    <li>
      <div class="skill-level-effect">
        <span class="level">1레벨</span>
        <span class="effect-text">방패 돌진으로 적을 밀어냅니다.</span>
      </div>
    </li>
  </ul>
</div>
</body></html>
"""


def make_mock_response(html: str) -> MagicMock:
    """requests.Response를 흉내 내는 Mock 객체를 반환합니다."""
    mock_resp = MagicMock()
    mock_resp.text = html
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def test_extract_skills_returns_dataframe():
    """extract_skills가 DataFrame을 반환하는지 확인 (HTTP 모킹)."""
    with patch("requests.get", return_value=make_mock_response(MOCK_SKILL_HTML)):
        from game_chatbot_data.crawlers.skills import extract_skills
        result = extract_skills(job_code=102, limit=1)

    # 반환값이 튜플(level_df, summary_df, tripod_df) 형태인지 확인
    assert isinstance(result, tuple)
    assert len(result) == 3
    for df in result:
        assert isinstance(df, pd.DataFrame)


def test_mask_question_replaces_entity():
    """few_shot의 mask_question이 엔티티를 올바르게 치환하는지 확인."""
    from game_chatbot_data.embeddings.few_shot import mask_question

    result = mask_question("황로드유의 원한 각인 효과가 뭐야?")
    assert "[CHARACTER_NAME]" in result
    assert "[ENGRAVING_NAME]" in result
    assert "황로드유" not in result
    assert "원한" not in result


def test_mask_question_longest_match_first():
    """긴 단어가 먼저 치환되어 오치환이 없는지 확인 (결투의 대가 vs 대가)."""
    from game_chatbot_data.embeddings.few_shot import mask_question

    result = mask_question("결투의 대가 각인 알려줘")
    assert "[ENGRAVING_NAME]" in result
    # '결투의 대가' 전체가 치환됐어야 함 (부분 치환 방지)
    assert "결투의" not in result
