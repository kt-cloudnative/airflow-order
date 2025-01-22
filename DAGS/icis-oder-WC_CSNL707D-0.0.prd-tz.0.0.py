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
                , WORKFLOW_NAME='WC_CSNL707D',WORKFLOW_ID='924a22b0d5e04f909a55a1f8709227ce', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSNL707D-0.0.prd-tz.0.0'
    ,'schedule_interval':'3 * * * *'
    ,'start_date': datetime(2024, 11, 18, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('924a22b0d5e04f909a55a1f8709227ce')

    csnl707dJob_vol = []
    csnl707dJob_volMnt = []
    csnl707dJob_env = [getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap2'), getICISSecret('icis-oder-wrlincomn-batch-secret')]
    csnl707dJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnl707dJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnl707dJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ec93faf486ec4b2a95e2f82792ee07a5',
        'volumes': csnl707dJob_vol,
        'volume_mounts': csnl707dJob_volMnt,
        'env_from':csnl707dJob_env,
        'task_id':'csnl707dJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.4',
        'arguments':["--job.name=csnl707dJob", "testBatchRequestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('924a22b0d5e04f909a55a1f8709227ce')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnl707dJob,
        Complete
    ]) 

    # authCheck >> csnl707dJob >> Complete
    workflow








