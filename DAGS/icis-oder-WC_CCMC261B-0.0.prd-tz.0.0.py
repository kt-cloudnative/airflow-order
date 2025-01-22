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
                , WORKFLOW_NAME='WC_CCMC261B',WORKFLOW_ID='b77bdfb3ed134ea4ac68e043ca4973aa', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CCMC261B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 2, 19, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b77bdfb3ed134ea4ac68e043ca4973aa')

    ccmc261bJob_vol = []
    ccmc261bJob_volMnt = []
    ccmc261bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    ccmc261bJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    ccmc261bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    ccmc261bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ceee18d96ce643b2a2a47510f7a39951',
        'volumes': ccmc261bJob_vol,
        'volume_mounts': ccmc261bJob_volMnt,
        'env_from':ccmc261bJob_env,
        'task_id':'ccmc261bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.5',
        'arguments':["--job.name=ccmc261bJob", "pgmNm=ccmc261b", "procNo=8", "checkBill=''", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b77bdfb3ed134ea4ac68e043ca4973aa')

    workflow = COMMON.getICISPipeline([
        authCheck,
        ccmc261bJob,
        Complete
    ]) 

    # authCheck >> ccmc261bJob >> Complete
    workflow








