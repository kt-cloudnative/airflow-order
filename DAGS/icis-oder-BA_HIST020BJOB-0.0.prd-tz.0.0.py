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
                , WORKFLOW_NAME='BA_HIST020BJOB',WORKFLOW_ID='eeb524a3843f4b619d07e229db1b4b75', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BA_HIST020BJOB-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 12, 12, 0, 3, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('eeb524a3843f4b619d07e229db1b4b75')

    hist020bJob_vol = []
    hist020bJob_volMnt = []
    hist020bJob_vol.append(getVolume('shared-volume','shared-volume'))
    hist020bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    hist020bJob_env = [getICISConfigMap('icis-oder-batch-sample-test-configmap'), getICISConfigMap('icis-oder-batch-sample-test-configmap2'), getICISSecret('icis-oder-batch-sample-test-secret')]
    hist020bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    hist020bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    hist020bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5db08f858284425cbd4cc64e75b747bc',
        'volumes': hist020bJob_vol,
        'volume_mounts': hist020bJob_volMnt,
        'env_from':hist020bJob_env,
        'task_id':'hist020bJob',
        'image':'/icis/icis-oder-batch-sample-test:0.7.1.2',
        'arguments':["--job.name=hist020bJob", "ignoredJobs=hist010b-hist020b-redis", "workEnv=PRD"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('eeb524a3843f4b619d07e229db1b4b75')

    workflow = COMMON.getICISPipeline([
        authCheck,
        hist020bJob,
        Complete
    ]) 

    # authCheck >> hist020bJob >> Complete
    workflow








