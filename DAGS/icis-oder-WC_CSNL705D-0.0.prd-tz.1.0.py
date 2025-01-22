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
                , WORKFLOW_NAME='WC_CSNL705D',WORKFLOW_ID='021ab2c1a7624dbebb500e24b4f1570b', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSNL705D-0.0.prd-tz.1.0'
    ,'schedule_interval':'*/3 * * * *'
    ,'start_date': datetime(2024, 12, 19, 10, 50, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('021ab2c1a7624dbebb500e24b4f1570b')

    csnl705dJob_vol = []
    csnl705dJob_volMnt = []
    csnl705dJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnl705dJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csnl705dJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnl705dJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f7489ae52f5f45d6a71bf39f55951348',
        'volumes': csnl705dJob_vol,
        'volume_mounts': csnl705dJob_volMnt,
        'env_from':csnl705dJob_env,
        'task_id':'csnl705dJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.5',
        'arguments':["--job.name=csnl705dJob", "testBatchRequestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('021ab2c1a7624dbebb500e24b4f1570b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnl705dJob,
        Complete
    ]) 

    # authCheck >> csnl705dJob >> Complete
    workflow








