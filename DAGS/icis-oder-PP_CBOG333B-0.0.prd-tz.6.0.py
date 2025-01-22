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
                , WORKFLOW_NAME='PP_CBOG333B',WORKFLOW_ID='dc08d690cf6e4c169ec3a02ec5113532', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG333B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('dc08d690cf6e4c169ec3a02ec5113532')

    cbog333bJob_vol = []
    cbog333bJob_volMnt = []
    cbog333bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog333bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog333bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog333bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog333bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog333bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8cf916ed0ece4a0295fbe3a1783a9096',
        'volumes': cbog333bJob_vol,
        'volume_mounts': cbog333bJob_volMnt,
        'env_from':cbog333bJob_env,
        'task_id':'cbog333bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog333bJob", "endTranDate=20241129", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('dc08d690cf6e4c169ec3a02ec5113532')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog333bJob,
        Complete
    ]) 

    # authCheck >> cbog333bJob >> Complete
    workflow








