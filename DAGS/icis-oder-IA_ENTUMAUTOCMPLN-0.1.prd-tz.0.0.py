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
                , WORKFLOW_NAME='IA_ENTUMAUTOCMPLN',WORKFLOW_ID='3654ed6c17694889a1d3f5f17ca63d1d', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_ENTUMAUTOCMPLN-0.1.prd-tz.0.0'
    ,'schedule_interval':'30 22 * * *'
    ,'start_date': datetime(2024, 9, 25, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3654ed6c17694889a1d3f5f17ca63d1d')

    entumAutoCmplnJob_vol = []
    entumAutoCmplnJob_volMnt = []
    entumAutoCmplnJob_env = [getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISConfigMap('icis-oder-inetaplca-batch-configmap2'), getICISSecret('icis-oder-inetaplca-batch-secret')]
    entumAutoCmplnJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    entumAutoCmplnJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    entumAutoCmplnJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '44d7b1851bd3459187e9a7e7d29a7245',
        'volumes': entumAutoCmplnJob_vol,
        'volume_mounts': entumAutoCmplnJob_volMnt,
        'env_from':entumAutoCmplnJob_env,
        'task_id':'entumAutoCmplnJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.24',
        'arguments':["--job.name=cbot964bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3654ed6c17694889a1d3f5f17ca63d1d')

    workflow = COMMON.getICISPipeline([
        authCheck,
        entumAutoCmplnJob,
        Complete
    ]) 

    # authCheck >> entumAutoCmplnJob >> Complete
    workflow








