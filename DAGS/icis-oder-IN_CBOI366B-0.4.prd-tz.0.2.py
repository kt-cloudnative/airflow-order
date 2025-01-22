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

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='IN_CBOI366B',WORKFLOW_ID='8ea9e10bd12f4d56bb05fe9c91e8f94a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI366B-0.4.prd-tz.0.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 10, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8ea9e10bd12f4d56bb05fe9c91e8f94a')

    cboi366bJob_vol = []
    cboi366bJob_volMnt = []
    cboi366bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboi366bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboi366bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cboi366bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cboi366bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi366bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8835e43f3255404183375b7e2e23049e',
        'volumes': cboi366bJob_vol,
        'volume_mounts': cboi366bJob_volMnt,
        'env_from':cboi366bJob_env,
        'task_id':'cboi366bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.1',
        'arguments':["--job.name=cboi366bJob",  "rcvEndDate=20241026", "regerEmpOfc=MNC013", "regerEmpNo=91356994", "regerEmpName=Kang", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8ea9e10bd12f4d56bb05fe9c91e8f94a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi366bJob,
        Complete
    ]) 

    # authCheck >> cboi366bJob >> Complete
    workflow








