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
                , WORKFLOW_NAME='BI_CSNL400B',WORKFLOW_ID='2fc25cca883f4a1282ae04531ff06806', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSNL400B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 11, 3, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2fc25cca883f4a1282ae04531ff06806')

    csnl400bJob_vol = []
    csnl400bJob_volMnt = []
    csnl400bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnl400bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    csnl400bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnl400bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0923eb4742a743eca560111e7303d604',
        'volumes': csnl400bJob_vol,
        'volume_mounts': csnl400bJob_volMnt,
        'env_from':csnl400bJob_env,
        'task_id':'csnl400bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.5',
        'arguments':["--job.name=csnl400bJob", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2fc25cca883f4a1282ae04531ff06806')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnl400bJob,
        Complete
    ]) 

    # authCheck >> csnl400bJob >> Complete
    workflow








