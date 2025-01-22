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
                , WORKFLOW_NAME='IA_CBOT958B',WORKFLOW_ID='4db56430290a485a8bc4bbfe22d6a300', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT958B-0.2.prd-tz.1.0'
    ,'schedule_interval':'20 20 * * *'
    ,'start_date': datetime(2024, 7, 16, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4db56430290a485a8bc4bbfe22d6a300')

    cbot958bJob_vol = []
    cbot958bJob_volMnt = []
    cbot958bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot958bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot958bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot958bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c7da6434000a44be8adf025b87f0beb3',
        'volumes': cbot958bJob_vol,
        'volume_mounts': cbot958bJob_volMnt,
        'env_from':cbot958bJob_env,
        'task_id':'cbot958bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.35',
        'arguments':["--job.name=cbot958bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4db56430290a485a8bc4bbfe22d6a300')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot958bJob,
        Complete
    ]) 

    # authCheck >> cbot958bJob >> Complete
    workflow








