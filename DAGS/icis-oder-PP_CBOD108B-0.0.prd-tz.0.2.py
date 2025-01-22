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
                , WORKFLOW_NAME='PP_CBOD108B',WORKFLOW_ID='09c1e8b282024c90aaa6b28c33dc4729', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD108B-0.0.prd-tz.0.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 16, 13, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('09c1e8b282024c90aaa6b28c33dc4729')

    cbod108bJob_vol = []
    cbod108bJob_volMnt = []
    cbod108bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbod108bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod108bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod108bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '71b33872dd4f4611b69093bc8dc1bee9',
        'volumes': cbod108bJob_vol,
        'volume_mounts': cbod108bJob_volMnt,
        'env_from':cbod108bJob_env,
        'task_id':'cbod108bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.45',
        'arguments':["--job.name=cbod108bJob", "workDate=20240731", "saveSaId=11778599831","saveOrdNo=23002AT03694092",  "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('09c1e8b282024c90aaa6b28c33dc4729')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod108bJob,
        Complete
    ]) 

    # authCheck >> cbod108bJob >> Complete
    workflow








