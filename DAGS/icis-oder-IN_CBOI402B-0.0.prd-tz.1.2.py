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
                , WORKFLOW_NAME='IN_CBOI402B',WORKFLOW_ID='7576299861f546e7afc9d10336faaac1', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI402B-0.0.prd-tz.1.2'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 13, 18, 31, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7576299861f546e7afc9d10336faaac1')

    cboi402bJob_vol = []
    cboi402bJob_volMnt = []
    cboi402bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboi402bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cboi402bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi402bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6bcc4649549a4338843a1c15352187ee',
        'volumes': cboi402bJob_vol,
        'volume_mounts': cboi402bJob_volMnt,
        'env_from':cboi402bJob_env,
        'task_id':'cboi402bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.7',
        'arguments':["--job.name=cboi402bJob", "endTranDate=20241129", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7576299861f546e7afc9d10336faaac1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi402bJob,
        Complete
    ]) 

    # authCheck >> cboi402bJob >> Complete
    workflow








