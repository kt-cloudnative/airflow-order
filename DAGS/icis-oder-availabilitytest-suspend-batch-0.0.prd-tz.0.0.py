from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.sensors.time_delta import TimeDeltaSensor, TimeDeltaSensorAsync
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='availabilitytest-suspend-batch',WORKFLOW_ID='dbe353be3b6b4027b9b0ca191e161629', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-availabilitytest-suspend-batch-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 27, 4, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    # Airflow 설정
    AIRFLOW_HOST = "https://airflow.icis.kt.co.kr"
    API_ENDPOINT = f"{AIRFLOW_HOST}/api/v1"

    def get_keycloak_token():
        """Keycloak 토큰을 가져옵니다."""
        keycloak_host = "https://keycloak.icis.kt.co.kr"
        realm = "icis"
        client_id = "airflow"
        client_secret = "CCf7VWWziJ3y9kwxqgPpSgIWu3rbu2Qm"
        username = "admin"
        password = "new1234!"
        token_url = f"{keycloak_host}/realms/{realm}/protocol/openid-connect/token"

        data = {
            "grant_type": "password",
            "client_id": client_id,
            "client_secret": client_secret,
            "username": username,
            "password": password
        }
        response = requests.post(token_url, data=data)
        response.raise_for_status()

        return response.json()["access_token"]
    

    def api_request(method, endpoint, **kwargs):
        """Keycloak 토큰을 사용하여 인증된 API 요청을 보냅니다."""
        token = get_keycloak_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        response = requests.request(
            method, 
            f"{API_ENDPOINT}{endpoint}",
            headers=headers,
            **kwargs
        )
        response.raise_for_status()
        return response.json()
    
    def get_filtered_dags():
        """필터링된 DAG 목록을 가져옵니다 (페이지네이션 처리)."""
        offset = 0
        limit = 100  # API의 최대 제한값으로 설정
        all_dags = []

        # 필터 파라미터 설정
        filter_params = {
            'dag_id_pattern': f"icis-{COMMON.DOMAIN}-%.{COMMON.ENV}.%",
        }

        print(f"filter_params: {filter_params}")

        while True:
            # URL 파라미터 구성
            params = {
                "limit": limit,
                "offset": offset,
                **filter_params
            }
            
            response = api_request("GET", "/dags", params=params)
            dags = response.get('dags', [])
            if not dags:
                break

            # 활성화된 DAG만 필터링
            # active_dags = [dag['dag_id'] for dag in dags if not dag['is_paused']]
            # all_dags.extend(active_dags)

            # 활성화된 DAG만 필터링하고 'availabilitytest'를 포함하지 않는 DAG만 선택
            active_dags = [dag['dag_id'] for dag in dags if not dag['is_paused'] and 'availabilitytest' not in dag['dag_id']]
            all_dags.extend(active_dags)
            
            offset += limit
            if len(dags) < limit:
                break

        print(f"Total DAGs retrieved: {len(all_dags)}")
        return all_dags

    def pause_active(**context):
        """활성화된 DAG들을 일시 정지하고 목록을 저장합니다."""
        all_dags = get_filtered_dags()
        
        for dag_id in all_dags:
            print(f"Pausing DAG: {dag_id}")
            api_request("PATCH", f"/dags/{dag_id}", json={"is_paused": True})
        
        # Variable
        Variable.set(
            # Variable_Id
            f"24x7_{COMMON.DOMAIN}_{COMMON.ENV}_paused_dags",
            # Variable_Value
            json.dumps(all_dags),
            # Variable_Description
            (
                f"24x7 Test"
                f", List of {len(all_dags)} paused DAGs"
                f", Last updated: {datetime.now(local_tz).strftime('%Y-%m-%d %H:%M:%S')}"
            )
        )
        
        print(f"Paused {len(all_dags)} DAGs. IDs saved for later reactivation.")

    def unpause_saved(**context):
        """저장된 DAG 목록을 다시 활성화합니다."""
        paused_dags = json.loads(Variable.get(f"24x7_{COMMON.DOMAIN}_{COMMON.ENV}_paused_dags", "[]"))
        print(f"unpause_saved > paused_dags: {len(paused_dags)}")
        
        for dag_id in paused_dags:
            print(f"Unpausing DAG: {dag_id}")
            api_request("PATCH", f"/dags/{dag_id}", json={"is_paused": False})

        # Variable
        Variable.set(
            # Variable_Id
            f"24x7_{COMMON.DOMAIN}_{COMMON.ENV}_unpaused_dags",
            # Variable_Value
            json.dumps(paused_dags),
            # Variable_Description
            (
                f"24x7 Test"
                f", List of {len(paused_dags)} unpaused DAGs"
                f", Last updated: {datetime.now(local_tz).strftime('%Y-%m-%d %H:%M:%S')}"
            )
        )

        # print(f"Unpaused {len(paused_dags)} DAGs.")
        # Variable.delete(f"24x7_{COMMON.DOMAIN}_{COMMON.ENV}_paused_dags")

    # 태스크 정의
    pause_task = PythonOperator(
        task_id='pause_task',
        python_callable=pause_active
    )
    
    wait_for_time_delta_task = TimeDeltaSensorAsync(
        task_id='wait_for_time_delta_task',
        delta=timedelta(hours=4),
        mode='reschedule'
    )

    unpause_task = PythonOperator(
        task_id='unpause_task',
        python_callable=unpause_saved
    )

    # 태스크 순서 설정
    pause_task >> wait_for_time_delta_task >> unpause_task

