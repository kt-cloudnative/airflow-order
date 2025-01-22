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
                , WORKFLOW_NAME='PP_CSNG356B',WORKFLOW_ID='0eb9e71332674915a8c5f5dbbbc9aa1f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG356B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 15, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0eb9e71332674915a8c5f5dbbbc9aa1f')

    csng356bJob_vol = []
    csng356bJob_volMnt = []
    csng356bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng356bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng356bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng356bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng356bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng356bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b14cf2e8e4de4a2488520f92e4b69cd4',
        'volumes': csng356bJob_vol,
        'volume_mounts': csng356bJob_volMnt,
        'env_from':csng356bJob_env,
        'task_id':'csng356bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng356bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0eb9e71332674915a8c5f5dbbbc9aa1f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng356bJob,
        Complete
    ]) 

    # authCheck >> csng356bJob >> Complete
    workflow








