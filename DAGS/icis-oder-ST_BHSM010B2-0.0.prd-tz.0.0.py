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
                , WORKFLOW_NAME='ST_BHSM010B2',WORKFLOW_ID='577af3441e3f440ea68a62f80a65cfc1', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ST_BHSM010B2-0.0.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 17, 15, 33, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('577af3441e3f440ea68a62f80a65cfc1')

    bhsm010bJob_vol = []
    bhsm010bJob_volMnt = []
    bhsm010bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    bhsm010bJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    bhsm010bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    bhsm010bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6e92ebc7897846cdb2ae775b50613135',
        'volumes': bhsm010bJob_vol,
        'volume_mounts': bhsm010bJob_volMnt,
        'env_from':bhsm010bJob_env,
        'task_id':'bhsm010bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.4.1.79',
        'arguments':["--job.name=bhsm010bJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('577af3441e3f440ea68a62f80a65cfc1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        bhsm010bJob,
        Complete
    ]) 

    # authCheck >> bhsm010bJob >> Complete
    workflow








