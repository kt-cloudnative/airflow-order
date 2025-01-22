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
                , WORKFLOW_NAME='IN_CBOI888B',WORKFLOW_ID='0c2c531b7e304c0b981d34a6fdd9d9c9', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI888B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 3, 16, 4, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0c2c531b7e304c0b981d34a6fdd9d9c9')

    cboi888bJob_vol = []
    cboi888bJob_volMnt = []
    cboi888bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboi888bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboi888bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboi888bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cboi888bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi888bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9065105e012e498a95041ef343d99107',
        'volumes': cboi888bJob_vol,
        'volume_mounts': cboi888bJob_volMnt,
        'env_from':cboi888bJob_env,
        'task_id':'cboi888bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.6',
        'arguments':["--job.name=cboi888bJob", "count=0", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0c2c531b7e304c0b981d34a6fdd9d9c9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi888bJob,
        Complete
    ]) 

    # authCheck >> cboi888bJob >> Complete
    workflow








