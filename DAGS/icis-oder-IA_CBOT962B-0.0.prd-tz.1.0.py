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
                , WORKFLOW_NAME='IA_CBOT962B',WORKFLOW_ID='7d1a4fd7ce474101bf8e45fbff831d2b', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT962B-0.0.prd-tz.1.0'
    ,'schedule_interval':'20 20 * * *'
    ,'start_date': datetime(2024, 7, 12, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7d1a4fd7ce474101bf8e45fbff831d2b')

    cbot962bJob_vol = []
    cbot962bJob_volMnt = []
    cbot962bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot962bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot962bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot962bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '89c625ebeb054e679f8fd7d0f236b26e',
        'volumes': cbot962bJob_vol,
        'volume_mounts': cbot962bJob_volMnt,
        'env_from':cbot962bJob_env,
        'task_id':'cbot962bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.35',
        'arguments':["--job.name=cbot962bJob", "requestDate="+str(datetime.now()), "procDate="+str(datetime.now().strftime("%Y%m%d"))],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7d1a4fd7ce474101bf8e45fbff831d2b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot962bJob,
        Complete
    ]) 

    # authCheck >> cbot962bJob >> Complete
    workflow








