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
                , WORKFLOW_NAME='BI_CSNL921D',WORKFLOW_ID='3b77698828304350a1485d848d46875f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSNL921D-0.0.prd-tz.0.0'
    ,'schedule_interval':'3 * * * *'
    ,'start_date': datetime(2024, 11, 18, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3b77698828304350a1485d848d46875f')

    csnl921dJob_vol = []
    csnl921dJob_volMnt = []
    csnl921dJob_env = [getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISConfigMap('icis-oder-baseinfo-batch-configmap2'), getICISSecret('icis-oder-baseinfo-batch-secret')]
    csnl921dJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnl921dJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnl921dJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3c88d4e90b9d4424a56c757baccd6603',
        'volumes': csnl921dJob_vol,
        'volume_mounts': csnl921dJob_volMnt,
        'env_from':csnl921dJob_env,
        'task_id':'csnl921dJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.3',
        'arguments':["--job.name=csnl921dJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3b77698828304350a1485d848d46875f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnl921dJob,
        Complete
    ]) 

    # authCheck >> csnl921dJob >> Complete
    workflow








