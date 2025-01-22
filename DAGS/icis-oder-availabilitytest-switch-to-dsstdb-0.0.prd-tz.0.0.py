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
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_oc import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='availabilitytest-switch-to-dsstdb',WORKFLOW_ID='f254709c42714fdf8f960cfdb19e620f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-availabilitytest-switch-to-dsstdb-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 27, 4, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f254709c42714fdf8f960cfdb19e620f')

    switchtodsstdb_vol = []
    switchtodsstdb_volMnt = []
    switchtodsstdb_env = []


    switchtodsstdb = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '644e0fd5b4904ba7b5c8bf92c0ff15cf',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'switchtodsstdb',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-order | while read conf a; do oc patch configmap $conf -n t-order --type='json' -p=\"[{\\\"op\\\": \\\"replace\\\", \\\"path\\\": \\\"/data/DB_URL\\\", \\\"value\\\": \\\"$(oc get configmap $conf -n t-order -o=jsonpath='{.data.DB_URL_SCND}')\\\"}]\"; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    switchtodsstdbname = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '644e0fd5b4904ba7b5c8bf92c0ff15cf',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'switchtodsstdbname',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-order | while read conf a; do oc patch configmap $conf -n t-order --type='json' -p='[{\"op\": \"replace\", \"path\": \"/data/ACTIVE_DB\", \"value\": \"DB_URL_SCND\" }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      
    switchtodsstdblt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '644e0fd5b4904ba7b5c8bf92c0ff15cf',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'switchtodsstdblt',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-order-lt | while read conf _; do oc patch configmap $conf -n t-order-lt --type='json' -p=\"[{\\\"op\\\": \\\"replace\\\", \\\"path\\\": \\\"/data/DB_URL\\\", \\\"value\\\": \\\"$(oc get configmap $conf -n t-order-lt -o=jsonpath='{.data.DB_URL_SCND}')\\\"}]\"; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    switchtodsstdbnamelt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '644e0fd5b4904ba7b5c8bf92c0ff15cf',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'switchtodsstdbnamelt',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-order-lt | while read conf a; do oc patch configmap $conf -n t-order-lt --type='json' -p='[{\"op\": \"replace\", \"path\": \"/data/ACTIVE_DB\", \"value\": \"DB_URL_SCND\" }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    restartdsstdbpod = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '644e0fd5b4904ba7b5c8bf92c0ff15cf',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'restartdsstdbpod',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get rollout -l devpilot/type=online --no-headers -n t-order | while read rollname a; do oc patch rollout $rollname -n t-order --type='json' -p='[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env/-\", \"value\": {\"name\": \"DB_PATCH\", \"value\": \"new_value\"} }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    restartdsstdbpodlt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '644e0fd5b4904ba7b5c8bf92c0ff15cf',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'restartdsstdbpodlt',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get rollout -l devpilot/type=online --no-headers -n t-order-lt | while read rollname a; do oc patch rollout $rollname -n t-order-lt --type='json' -p='[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env/-\", \"value\": {\"name\": \"DB_PATCH\", \"value\": \"new_value\"} }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

       
      

    Complete = COMMON.getICISCompleteWflowTask('f254709c42714fdf8f960cfdb19e620f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        switchtodsstdb,
        switchtodsstdbname,
        switchtodsstdblt,
        switchtodsstdbnamelt,
        restartdsstdbpod,
        restartdsstdbpodlt,
        Complete
    ]) 

    # authCheck >> switchtodsstdb >> Complete
    workflow








