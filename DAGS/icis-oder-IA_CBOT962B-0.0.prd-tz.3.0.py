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
                , WORKFLOW_NAME='IA_CBOT962B',WORKFLOW_ID='834b1b3eb7a24b3887571e24b5e83cc2', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT962B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 35, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('834b1b3eb7a24b3887571e24b5e83cc2')

    cbot962bJob_vol = []
    cbot962bJob_volMnt = []
    cbot962bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot962bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot962bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot962bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot962bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot962bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '72785de139dc4f908494b4a474d68a57',
        'volumes': cbot962bJob_vol,
        'volume_mounts': cbot962bJob_volMnt,
        'env_from':cbot962bJob_env,
        'task_id':'cbot962bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.7.1.5',
        'arguments':["--job.name=cbot962bJob", "requestDate="+str(datetime.now()), "procDate=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('834b1b3eb7a24b3887571e24b5e83cc2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot962bJob,
        Complete
    ]) 

    # authCheck >> cbot962bJob >> Complete
    workflow








