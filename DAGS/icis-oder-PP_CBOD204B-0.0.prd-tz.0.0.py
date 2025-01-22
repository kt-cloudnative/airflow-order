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
                , WORKFLOW_NAME='PP_CBOD204B',WORKFLOW_ID='cb2101caa84f4fc8be31e3497ba182d2', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD204B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 10, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('cb2101caa84f4fc8be31e3497ba182d2')

    cbod204bJob_vol = []
    cbod204bJob_volMnt = []
    cbod204bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod204bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod204bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbod204bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod204bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod204bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '57d415506e4b43ada8eda478cd084587',
        'volumes': cbod204bJob_vol,
        'volume_mounts': cbod204bJob_volMnt,
        'env_from':cbod204bJob_env,
        'task_id':'cbod204bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.45',
        'arguments':["--job.name=cbod204bJob",
"endTranDate=20241020",
"fromTime=19",
"toTime=19",
"date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('cb2101caa84f4fc8be31e3497ba182d2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod204bJob,
        Complete
    ]) 

    # authCheck >> cbod204bJob >> Complete
    workflow








