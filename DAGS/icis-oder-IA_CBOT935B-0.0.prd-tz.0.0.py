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
                , WORKFLOW_NAME='IA_CBOT935B',WORKFLOW_ID='aa92e7029f6d437eb1e7a264d03fd490', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT935B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 17, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('aa92e7029f6d437eb1e7a264d03fd490')

    cbot935bJob_vol = []
    cbot935bJob_volMnt = []
    cbot935bJob_env = [getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISConfigMap('icis-oder-inetaplca-batch-configmap2'), getICISSecret('icis-oder-inetaplca-batch-secret')]
    cbot935bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot935bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot935bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b9e2323280e84e68bf74c31cfed29d4d',
        'volumes': cbot935bJob_vol,
        'volume_mounts': cbot935bJob_volMnt,
        'env_from':cbot935bJob_env,
        'task_id':'cbot935bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.26',
        'arguments':["--job.name=cbot935bJob", "requestDate="+str(datetime.now()), "procDate="+str(datetime.now().strftime("%Y%m%d")), "argc=3", "argv=A"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('aa92e7029f6d437eb1e7a264d03fd490')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot935bJob,
        Complete
    ]) 

    # authCheck >> cbot935bJob >> Complete
    workflow








