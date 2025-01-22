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
                , WORKFLOW_NAME='BI_CSAB811B',WORKFLOW_ID='77c5c733e75044b58a950d3b71b76dc9', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSAB811B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('77c5c733e75044b58a950d3b71b76dc9')

    csab811bJob_vol = []
    csab811bJob_volMnt = []
    csab811bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csab811bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    csab811bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csab811bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b9c92489b9ee4b7ea3975486a49f34d5',
        'volumes': csab811bJob_vol,
        'volume_mounts': csab811bJob_volMnt,
        'env_from':csab811bJob_env,
        'task_id':'csab811bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.5',
        'arguments':["--job.name=csab811bJob", "workDate=20240718", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('77c5c733e75044b58a950d3b71b76dc9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csab811bJob,
        Complete
    ]) 

    # authCheck >> csab811bJob >> Complete
    workflow








