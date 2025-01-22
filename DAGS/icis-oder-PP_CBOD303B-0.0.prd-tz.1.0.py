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
                , WORKFLOW_NAME='PP_CBOD303B',WORKFLOW_ID='310d67a79f0e4244a5fb95e03798b573', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD303B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 4, 9, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('310d67a79f0e4244a5fb95e03798b573')

    cbod303bJob_vol = []
    cbod303bJob_volMnt = []
    cbod303bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod303bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod303bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbod303bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod303bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod303bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b5341f98420c4a03b866fd3472a26d58',
        'volumes': cbod303bJob_vol,
        'volume_mounts': cbod303bJob_volMnt,
        'env_from':cbod303bJob_env,
        'task_id':'cbod303bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.51',
        'arguments':["--job.name=cbod303bJob",
"requestDate="+str(datetime.now()),
"endTranDate=20240731"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('310d67a79f0e4244a5fb95e03798b573')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod303bJob,
        Complete
    ]) 

    # authCheck >> cbod303bJob >> Complete
    workflow








