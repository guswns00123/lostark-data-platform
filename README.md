# lostark-data-platform

로스트아크 게임 데이터 파이프라인 — Airflow DAG + 크롤러/임베딩 패키지

## Architecture

```
GitHub (main branch)
  │
  └── GitHub Actions CD
        │  push to main → SSH → GCP VM
        ▼
   GCP Compute Engine
     ├── /home/airflow/lostark-data-platform/   ← 레포 clone
     │     ├── dags/                            ← DAG 소스
     │     └── game_chatbot_data/               ← 크롤러/임베딩 패키지
     │
     └── /opt/airflow/dags-prod/               ← Airflow가 실제 읽는 폴더
           (rsync로 dags/ 내용 동기화)
```

## 브랜치 전략

| 브랜치 | 용도 |
|--------|------|
| `main` | 운영 배포 대상 (CD 자동 트리거) |
| `dev` | 개발 작업 통합 |
| `feature/*` | 기능 단위 개발 |

PR 흐름: `feature/*` → `dev` (CI 실행) → `main` (CI + CD 실행)

## Setup

### 로컬 개발

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-dev.txt

cp .env.example .env
# .env에 실제 DB 정보와 OpenAI API Key 입력
```

### GCP VM 최초 설정 (1회)

```bash
# 1. 레포 clone
cd /home/airflow
git clone https://github.com/계정명/lostark-data-platform.git
cp .env.example .env && nano .env

# 2. 가상환경 생성
python3 -m venv /home/airflow/venv
source /home/airflow/venv/bin/activate
pip install -r requirements.txt

# 3. Airflow dags-prod 폴더 생성
sudo mkdir -p /opt/airflow/dags-prod
sudo chown airflow:airflow /opt/airflow/dags-prod
# airflow.cfg: dags_folder = /opt/airflow/dags-prod

# 4. SSH 키 생성 (GitHub Actions CD용)
ssh-keygen -t ed25519 -C "github-actions" -f ~/.ssh/github_actions_key
cat ~/.ssh/github_actions_key.pub >> ~/.ssh/authorized_keys
# cat ~/.ssh/github_actions_key  → GitHub Secret GCP_SSH_PRIVATE_KEY에 등록
```

### GitHub Secrets 등록

레포 > Settings > Secrets and variables > Actions에서 등록:

| Secret | 값 |
|--------|-----|
| `GCP_VM_HOST` | VM 외부 IP |
| `GCP_VM_USER` | VM 접속 유저명 (예: airflow) |
| `GCP_SSH_PRIVATE_KEY` | SSH 개인키 전체 내용 |

## Usage

```bash
# 크롤러 직접 실행 (수동)
python -m game_chatbot_data.crawlers.skills
python -m game_chatbot_data.crawlers.ark_passive
python -m game_chatbot_data.crawlers.engrave
python -m game_chatbot_data.crawlers.rune

# 임베딩 적재
python -m game_chatbot_data.embeddings.few_shot
python -m game_chatbot_data.embeddings.schema_embed

# 테스트
pytest tests/ -v

# Lint
ruff check .
```

## DAG 실행 (Airflow UI)

모든 DAG는 `is_paused_upon_creation=True`, `schedule=None`으로 설정되어 있습니다.
- 배포 후 Airflow UI에서 수동으로 Toggle(enable) 후 Trigger DAG로 실행

### dev 모드 전환 (테스트용)

Airflow UI > Admin > Variables에서 `environment = dev` 설정 시
직업 코드 1개(워로드)만 처리합니다.

## Project Structure

```
lostark-data-platform/
├── dags/                        # Airflow DAG 파일
├── game_chatbot_data/           # 크롤러 + 임베딩 Python 패키지
│   ├── config.py
│   ├── db.py
│   ├── crawlers/
│   └── embeddings/
├── tests/                       # 유닛 테스트
├── .github/workflows/           # CI/CD 워크플로우
│   ├── ci.yml                   # PR 시 lint + 테스트
│   └── cd.yml                   # main push 시 GCP 자동 배포
├── .env.example
└── requirements-dev.txt
```
