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
                , WORKFLOW_NAME='IC_CSNJ189B',WORKFLOW_ID='1636971d4936414e966676ec59e64e45', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CSNJ189B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 51, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1636971d4936414e966676ec59e64e45')

    csnj189bJob_vol = []
    csnj189bJob_volMnt = []
    csnj189bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnj189bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnj189bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnj189bJob_env.extend([getICISConfigMap('icis-oder-infocomm-batch-mng-configmap'), getICISSecret('icis-oder-infocomm-batch-mng-secret'), getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISSecret('icis-oder-infocomm-batch-secret')])
    csnj189bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnj189bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e4d327c276554a54b386b34877f7b3e3',
        'volumes': csnj189bJob_vol,
        'volume_mounts': csnj189bJob_volMnt,
        'env_from':csnj189bJob_env,
        'task_id':'csnj189bJob',
        'image':'/icis/icis-oder-infocomm-batch:0.7.1.6',
        'arguments':["--job.name=csnj189bJob", "reqDate=${YYYYMMDDHHMISS}", "procDate=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1636971d4936414e966676ec59e64e45')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnj189bJob,
        Complete
    ]) 

    # authCheck >> csnj189bJob >> Complete
    workflow








