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
                , WORKFLOW_NAME='IA_ENTUMAUTOCMPLN',WORKFLOW_ID='f2e7a2e48c994f5487dd38b5109e8e02', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_ENTUMAUTOCMPLN-0.1.prd-tz.1.0'
    ,'schedule_interval':'30 22 * * *'
    ,'start_date': datetime(2024, 9, 25, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f2e7a2e48c994f5487dd38b5109e8e02')

    entumAutoCmplnJob_vol = []
    entumAutoCmplnJob_volMnt = []
    entumAutoCmplnJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    entumAutoCmplnJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    entumAutoCmplnJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    entumAutoCmplnJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '7bc301f4c4024bebbc7b92e944b0a4c6',
        'volumes': entumAutoCmplnJob_vol,
        'volume_mounts': entumAutoCmplnJob_volMnt,
        'env_from':entumAutoCmplnJob_env,
        'task_id':'entumAutoCmplnJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.35',
        'arguments':["--job.name=cbot964bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f2e7a2e48c994f5487dd38b5109e8e02')

    workflow = COMMON.getICISPipeline([
        authCheck,
        entumAutoCmplnJob,
        Complete
    ]) 

    # authCheck >> entumAutoCmplnJob >> Complete
    workflow








