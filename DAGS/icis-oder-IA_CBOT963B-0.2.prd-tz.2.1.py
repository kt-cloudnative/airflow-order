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
                , WORKFLOW_NAME='IA_CBOT963B',WORKFLOW_ID='baa8e6a324e4450abe85b108f203be79', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT963B-0.2.prd-tz.2.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 40, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('baa8e6a324e4450abe85b108f203be79')

    cbot963bJob_vol = []
    cbot963bJob_volMnt = []
    cbot963bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot963bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot963bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot963bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '876ae2336b934925904e810fa6976c17',
        'volumes': cbot963bJob_vol,
        'volume_mounts': cbot963bJob_volMnt,
        'env_from':cbot963bJob_env,
        'task_id':'cbot963bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.36',
        'arguments':["--job.name=cbot963bJob", "requestDate="+str(datetime.now()), "procDate=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('baa8e6a324e4450abe85b108f203be79')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot963bJob,
        Complete
    ]) 

    # authCheck >> cbot963bJob >> Complete
    workflow








