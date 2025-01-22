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
                , WORKFLOW_NAME='PP_CBOG367B',WORKFLOW_ID='6d5e7101c2cc43179ba51ceccf22f673', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG367B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6d5e7101c2cc43179ba51ceccf22f673')

    cbog367bJob_vol = []
    cbog367bJob_volMnt = []
    cbog367bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog367bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog367bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbog367bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbog367bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog367bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '7b22e13071ed4752a9a2847dec65d878',
        'volumes': cbog367bJob_vol,
        'volume_mounts': cbog367bJob_volMnt,
        'env_from':cbog367bJob_env,
        'task_id':'cbog367bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=cbog367bJob","requestDate=20240628", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6d5e7101c2cc43179ba51ceccf22f673')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog367bJob,
        Complete
    ]) 

    # authCheck >> cbog367bJob >> Complete
    workflow








