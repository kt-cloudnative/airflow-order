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
                , WORKFLOW_NAME='IN_HAP_DC_CURN',WORKFLOW_ID='4d8bc93e45cc4c468cd205ed69b8bf43', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_HAP_DC_CURN-0.4.prd-tz.0.0'
    ,'schedule_interval':'0 22 1 * *'
    ,'start_date': datetime(2025, 1, 10, 11, 15, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4d8bc93e45cc4c468cd205ed69b8bf43')

    cboi401bJob_vol = []
    cboi401bJob_volMnt = []
    cboi401bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboi401bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cboi401bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi401bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1560f0f750fa407fba36a118445b3014',
        'volumes': cboi401bJob_vol,
        'volume_mounts': cboi401bJob_volMnt,
        'env_from':cboi401bJob_env,
        'task_id':'cboi401bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.4.1.55',
        'arguments':["--job.name=cboi401bJob", "lobCd=IN", "requestDate=${YYYYMMDDHHMISSSSS}", "billYm=${YYYYMM}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4d8bc93e45cc4c468cd205ed69b8bf43')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi401bJob,
        Complete
    ]) 

    # authCheck >> cboi401bJob >> Complete
    workflow








