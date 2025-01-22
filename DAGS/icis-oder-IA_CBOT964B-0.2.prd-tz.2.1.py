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
                , WORKFLOW_NAME='IA_CBOT964B',WORKFLOW_ID='7a3c49a408504069af77ecb8fd23b752', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT964B-0.2.prd-tz.2.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7a3c49a408504069af77ecb8fd23b752')

    cbot964bJob_vol = []
    cbot964bJob_volMnt = []
    cbot964bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot964bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot964bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot964bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4a59a92d9f12400a83106a8db8593484',
        'volumes': cbot964bJob_vol,
        'volume_mounts': cbot964bJob_volMnt,
        'env_from':cbot964bJob_env,
        'task_id':'cbot964bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.36',
        'arguments':["--job.name=cbot964bJob", "requestDate="+str(datetime.now()), "procDate=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7a3c49a408504069af77ecb8fd23b752')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot964bJob,
        Complete
    ]) 

    # authCheck >> cbot964bJob >> Complete
    workflow








