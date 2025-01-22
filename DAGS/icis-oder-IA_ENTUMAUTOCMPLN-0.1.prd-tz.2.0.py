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
                , WORKFLOW_NAME='IA_ENTUMAUTOCMPLN',WORKFLOW_ID='17b82dfae7424e71a76a810fcbec1c48', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_ENTUMAUTOCMPLN-0.1.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('17b82dfae7424e71a76a810fcbec1c48')

    entumAutoCmplnJob_vol = []
    entumAutoCmplnJob_volMnt = []
    entumAutoCmplnJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    entumAutoCmplnJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    entumAutoCmplnJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    entumAutoCmplnJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0475da5a1ff84835b39f8d032ff140eb',
        'volumes': entumAutoCmplnJob_vol,
        'volume_mounts': entumAutoCmplnJob_volMnt,
        'env_from':entumAutoCmplnJob_env,
        'task_id':'entumAutoCmplnJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.36',
        'arguments':["--job.name=cbot964bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('17b82dfae7424e71a76a810fcbec1c48')

    workflow = COMMON.getICISPipeline([
        authCheck,
        entumAutoCmplnJob,
        Complete
    ]) 

    # authCheck >> entumAutoCmplnJob >> Complete
    workflow








