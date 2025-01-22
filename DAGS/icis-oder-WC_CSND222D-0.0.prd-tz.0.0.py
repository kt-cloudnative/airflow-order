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
                , WORKFLOW_NAME='WC_CSND222D',WORKFLOW_ID='4a6e66d3fb4b4156b2b10e1cf6b8e663', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSND222D-0.0.prd-tz.0.0'
    ,'schedule_interval':'3 * * * *'
    ,'start_date': datetime(2024, 11, 18, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4a6e66d3fb4b4156b2b10e1cf6b8e663')

    csnd222dJob_vol = []
    csnd222dJob_volMnt = []
    csnd222dJob_env = [getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap2'), getICISSecret('icis-oder-wrlincomn-batch-secret')]
    csnd222dJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd222dJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd222dJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ef20c3a6624c46a1a8df37732b06c522',
        'volumes': csnd222dJob_vol,
        'volume_mounts': csnd222dJob_volMnt,
        'env_from':csnd222dJob_env,
        'task_id':'csnd222dJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.4',
        'arguments':["--job.name=csnd222dJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4a6e66d3fb4b4156b2b10e1cf6b8e663')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd222dJob,
        Complete
    ]) 

    # authCheck >> csnd222dJob >> Complete
    workflow








