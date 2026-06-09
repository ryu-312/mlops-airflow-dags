"""
invest-train 학습 트리거 DAG
============================

invest-train 서비스의 POST /train 을 호출해 모델 재학습을 트리거한다.
/train 은 즉시 202(accepted)만 반환하고 실제 학습은 백그라운드로 도는 비동기 구조이므로,
DAG 는 다음 3단계로 구성한다.

  1) trigger_train   : POST /train  (202 정상 / 409 이미 실행중은 기존 런에 합류)
  2) wait_until_idle : GET /train/status 를 폴링하여 running==false 까지 대기
                       (reschedule 모드 → 대기 동안 워커 슬롯 점유 안 함)
  3) check_result    : last_result.status 가 success 인지 판정. 실패면 태스크 실패→알림

스케줄: 매일 KST 04:00 (Athena 데이터 적재 완료 후 ~ 업무 시작 전).
        서버가 동시 학습을 409 로 차단하므로 중복 트리거는 안전.

[Airflow Variables] (Admin → Variables 에서 설정, 미설정 시 기본값 사용)
  - invest_train_base_url : 학습 서비스 base URL (기본값: http://api.mlops.click)
        · 외부/타클러스터 Airflow(현 구성): http://api.mlops.click   ← ALB 경유, 기본값
        · mlops 클러스터 내부에서 실행하는 경우만: http://invest-train.mlops.svc.cluster.local:8080
        ※ Airflow 가 mlops 와 다른 클러스터면 cluster.local 내부 DNS 가 해석되지 않으므로
          반드시 외부 ALB URL 을 사용한다.
  - invest_train_token    : (선택) TRAIN_TOKEN 운영 시 X-Train-Token 헤더 값. 미설정이면 미전송.
"""

from __future__ import annotations

import logging

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.sensors.python import PythonSensor

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")

# 폴링/타임아웃 설정
POKE_INTERVAL_SEC = 60          # status 확인 주기
WAIT_TIMEOUT_SEC = 60 * 45      # 최대 대기(학습 시간 상한). 초과 시 sensor 실패
HTTP_TIMEOUT_SEC = 30           # 개별 HTTP 요청 타임아웃


def _base_url() -> str:
    # 기본값은 외부 ALB. Airflow 가 mlops 클러스터 밖이면 내부 DNS 가 안 풀리므로
    # ALB URL 을 사용한다. 같은 클러스터 내부 실행 시에만 Variable 로 내부 URL 지정.
    return Variable.get(
        "invest_train_base_url",
        default_var="http://api.mlops.click",
    ).rstrip("/")


def _headers() -> dict:
    token = Variable.get("invest_train_token", default_var="").strip()
    return {"X-Train-Token": token} if token else {}


@dag(
    dag_id="trigger_invest_train",
    description="invest-train 모델 일일 재학습 트리거",
    schedule="None",                 #  
    start_date=pendulum.datetime(2026, 6, 9, tz=KST),
    catchup=False,                        # 과거 미실행분 몰아서 실행 금지
    max_active_runs=1,                    # DAG 런 중첩 금지(학습은 단일 실행)
    default_args={
        "owner": "mlops",
        "retries": 0,                     # 학습 재시도는 다음 스케줄/수동 트리거로
    },
    tags=["invest-train", "mlops", "training"],
)
def invest_train_daily():

    @task
    def trigger_train() -> str:
        """POST /train. 202=정상 트리거, 409=이미 실행중(기존 런에 합류해 폴링)."""
        url = f"{_base_url()}/train"
        resp = requests.post(url, headers=_headers(), timeout=HTTP_TIMEOUT_SEC)

        if resp.status_code == 202:
            started = resp.json().get("started_at")
            logger.info("학습 트리거 성공(202). started_at=%s", started)
            return started or ""

        if resp.status_code == 409:
            # 이미 실행 중 — 새 학습을 띄우지 않고 진행 중인 런이 끝날 때까지 폴링
            logger.warning("이미 학습이 실행 중(409). 기존 런 완료를 대기한다.")
            return ""

        if resp.status_code == 401:
            raise AirflowException("학습 토큰 인증 실패(401). invest_train_token Variable 확인.")

        raise AirflowException(
            f"예상치 못한 응답: HTTP {resp.status_code} body={resp.text[:300]}"
        )

    def _is_idle(**_) -> bool:
        """running==false 면 True(학습 종료). reschedule 모드로 슬롯 비점유."""
        url = f"{_base_url()}/train/status"
        resp = requests.get(url, headers=_headers(), timeout=HTTP_TIMEOUT_SEC)
        resp.raise_for_status()
        running = bool(resp.json().get("running", False))
        logger.info("폴링: running=%s", running)
        return not running

    wait_until_idle = PythonSensor(
        task_id="wait_until_idle",
        python_callable=_is_idle,
        mode="reschedule",                # 대기 동안 워커 슬롯 반납(권장)
        poke_interval=POKE_INTERVAL_SEC,
        timeout=WAIT_TIMEOUT_SEC,
        soft_fail=False,                  # 타임아웃 시 태스크 실패(알림)
    )

    @task
    def check_result() -> dict:
        """학습 결과 판정. last_result.status != success 면 실패시켜 알림."""
        url = f"{_base_url()}/train/status"
        resp = requests.get(url, headers=_headers(), timeout=HTTP_TIMEOUT_SEC)
        resp.raise_for_status()
        body = resp.json()
        result = body.get("last_result")

        if not result:
            raise AirflowException("last_result 가 비어 있음 — 학습이 기록되지 않았다.")

        status = result.get("status")
        if status == "success":
            logger.info(
                "학습 성공: rows=%s elapsed=%ss cls_ver=%s reg_ver=%s",
                result.get("rows"),
                result.get("elapsed_sec"),
                (result.get("classification") or {}).get("version"),
                (result.get("regression") or {}).get("version"),
            )
            return result

        # status == "error" 등 → 태스크 실패로 처리해 Airflow 알림 발생
        raise AirflowException(f"학습 실패: {result.get('error', result)}")

    started = trigger_train()
    started >> wait_until_idle >> check_result()


invest_train_daily()
