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
                , WORKFLOW_NAME='availabilitytest-stop-daemon',WORKFLOW_ID='a8168f3326854ae986636916702d4db2', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-availabilitytest-stop-daemon-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 27, 4, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a8168f3326854ae986636916702d4db2')

    stopdaemon_vol = []
    stopdaemon_volMnt = []
    stopdaemon_env = []


    stopdaemon = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c678cb3da8bb4ccc8bf3c7de7aade1cd',
        'volumes': stopdaemon_vol,
        'volume_mounts': stopdaemon_volMnt,
        'env_from':stopdaemon_env,
        'task_id':'stopdaemon',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get rollout -l devpilot/type=daemon --no-headers -n t-order | while read rollname a; do oc scale rollout $rollname -n t-order --replicas=0; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    stopdaemonlt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c678cb3da8bb4ccc8bf3c7de7aade1cd',
        'volumes': stopdaemon_vol,
        'volume_mounts': stopdaemon_volMnt,
        'env_from':stopdaemon_env,
        'task_id':'stopdaemonlt',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get rollout -l devpilot/type=daemon --no-headers -n t-order-lt | while read rollname a; do oc scale rollout $rollname -n t-order-lt --replicas=0; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
       
      

    Complete = COMMON.getICISCompleteWflowTask('a8168f3326854ae986636916702d4db2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        stopdaemon,
        stopdaemonlt,
        Complete
    ]) 

    # authCheck >> stopdaemon >> Complete
    workflow








