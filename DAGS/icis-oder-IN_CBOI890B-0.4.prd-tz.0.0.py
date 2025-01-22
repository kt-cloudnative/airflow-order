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
                , WORKFLOW_NAME='IN_CBOI890B',WORKFLOW_ID='cd3a4c6726d54557a96987d37d08375b', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI890B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 3, 16, 1, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('cd3a4c6726d54557a96987d37d08375b')

    cboi890bJob_vol = []
    cboi890bJob_volMnt = []
    cboi890bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboi890bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboi890bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboi890bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cboi890bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi890bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'dd37253217444b8b99d2c08d7841dba9',
        'volumes': cboi890bJob_vol,
        'volume_mounts': cboi890bJob_volMnt,
        'env_from':cboi890bJob_env,
        'task_id':'cboi890bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.6',
        'arguments':["--job.name=cboi890bJob", "count=0", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('cd3a4c6726d54557a96987d37d08375b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi890bJob,
        Complete
    ]) 

    # authCheck >> cboi890bJob >> Complete
    workflow








