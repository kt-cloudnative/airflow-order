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
                , WORKFLOW_NAME='IA_CBOT937B',WORKFLOW_ID='82066c73552342408279e48f2176291a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT937B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 17, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('82066c73552342408279e48f2176291a')

    cbot937bJob_vol = []
    cbot937bJob_volMnt = []
    cbot937bJob_env = [getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISConfigMap('icis-oder-inetaplca-batch-configmap2'), getICISSecret('icis-oder-inetaplca-batch-secret')]
    cbot937bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot937bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot937bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '499386c69a7b425b8b653e0cf4c49a07',
        'volumes': cbot937bJob_vol,
        'volume_mounts': cbot937bJob_volMnt,
        'env_from':cbot937bJob_env,
        'task_id':'cbot937bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.28',
        'arguments':["--job.name=cbot937bJob", "requestDate="+str(datetime.now()), "procDate="+str(datetime.now().strftime("%Y%m%d")), "argc=2"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('82066c73552342408279e48f2176291a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot937bJob,
        Complete
    ]) 

    # authCheck >> cbot937bJob >> Complete
    workflow








