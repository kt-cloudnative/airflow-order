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
                , WORKFLOW_NAME='BI_CSNL400D',WORKFLOW_ID='26b75ab9a08c4d989d5da8b6451aa284', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSNL400D-0.0.prd-tz.0.0'
    ,'schedule_interval':'3 * * * *'
    ,'start_date': datetime(2024, 9, 6, 8, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('26b75ab9a08c4d989d5da8b6451aa284')

    csnl400dJob_vol = []
    csnl400dJob_volMnt = []
    csnl400dJob_env = [getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISConfigMap('icis-oder-baseinfo-batch-configmap2'), getICISSecret('icis-oder-baseinfo-batch-secret')]
    csnl400dJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnl400dJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnl400dJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ad8f824883cd4ea4b1c86269ceb67ab4',
        'volumes': csnl400dJob_vol,
        'volume_mounts': csnl400dJob_volMnt,
        'env_from':csnl400dJob_env,
        'task_id':'csnl400dJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.3',
        'arguments':["--job.name=csnl400dJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('26b75ab9a08c4d989d5da8b6451aa284')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnl400dJob,
        Complete
    ]) 

    # authCheck >> csnl400dJob >> Complete
    workflow








