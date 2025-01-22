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
                , WORKFLOW_NAME='availabilitytest-switch-to-prddb',WORKFLOW_ID='df60bc2e313b4af9a5490c5617a72170', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-availabilitytest-switch-to-prddb-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 27, 4, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('df60bc2e313b4af9a5490c5617a72170')

    switchtoprddb_vol = []
    switchtoprddb_volMnt = []
    switchtoprddb_env = []


    startdaemon = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '445aeb3a77ad417fbae947fbc380dd68',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'startdaemon',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get rollout -l devpilot/type=daemon --no-headers -n t-order | while read rollname a; do oc scale rollout $rollname -n t-order --replicas=1; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
    startdaemonlt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '445aeb3a77ad417fbae947fbc380dd68',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'startdaemonlt',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get rollout -l devpilot/type=daemon --no-headers -n t-order-lt | while read rollname a; do oc scale rollout $rollname -n t-order-lt --replicas=1; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })


    switchtoprddb = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '445aeb3a77ad417fbae947fbc380dd68',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'switchtoprddb',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-order | while read conf a; do oc patch configmap $conf -n t-order --type='json' -p=\"[{\\\"op\\\": \\\"replace\\\", \\\"path\\\": \\\"/data/DB_URL\\\", \\\"value\\\": \\\"$(oc get configmap $conf -n t-order -o=jsonpath='{.data.DB_URL_PRMR}')\\\"}]\"; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    switchtoprddbname = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '445aeb3a77ad417fbae947fbc380dd68',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'switchtoprddbname',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-order | while read conf a; do oc patch configmap $conf -n t-order --type='json' -p='[{\"op\": \"replace\", \"path\": \"/data/ACTIVE_DB\", \"value\": \"DB_URL_PRMR\" }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      
    switchtoprddblt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '445aeb3a77ad417fbae947fbc380dd68',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'switchtoprddblt',
        'image':'/icis/icis-oder-availabilitytest:20240925102425',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-order-lt | while read conf _; do oc patch configmap $conf -n t-order-lt --type='json' -p=\"[{\\\"op\\\": \\\"replace\\\", \\\"path\\\": \\\"/data/DB_URL\\\", \\\"value\\\": \\\"$(oc get configmap $conf -n t-order-lt -o=jsonpath='{.data.DB_URL_PRMR}')\\\"}]\"; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    switchtoprddbnamelt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '445aeb3a77ad417fbae947fbc380dd68',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'switchtoprddbnamelt',
        'image':'/icis/icis-oder-availabilitytest:20240925102425',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-order-lt | while read conf a; do oc patch configmap $conf -n t-order-lt --type='json' -p='[{\"op\": \"replace\", \"path\": \"/data/ACTIVE_DB\", \"value\": \"DB_URL_PRMR\" }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    }) 
    
    restartprddbpod = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '445aeb3a77ad417fbae947fbc380dd68',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'restartprddbpod',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get rollout -l devpilot/type=online --no-headers -n t-order | while read rollname a; do oc patch rollout $rollname -n t-order --type='json' -p='[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env/-\", \"value\": {\"name\": \"DB_PATCH\", \"value\": \"new_value\"} }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    restartprddbpodlt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '445aeb3a77ad417fbae947fbc380dd68',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'restartprddbpodlt',
        'image':'/icis/origin-cli:1.0.2',
        'arguments':["oc get rollout -l devpilot/type=online --no-headers -n t-order-lt | while read rollname a; do oc patch rollout $rollname -n t-order-lt --type='json' -p='[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env/-\", \"value\": {\"name\": \"DB_PATCH\", \"value\": \"new_value\"} }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })


    Complete = COMMON.getICISCompleteWflowTask('df60bc2e313b4af9a5490c5617a72170')

    workflow = COMMON.getICISPipeline([
        authCheck,
        startdaemon,
        startdaemonlt,
        switchtoprddb,
        switchtoprddbname,
        switchtoprddblt,
        switchtoprddbnamelt,
        restartprddbpod,
        restartprddbpodlt,
        Complete
    ]) 

    # authCheck >> switchtoprddb >> Complete
    workflow








