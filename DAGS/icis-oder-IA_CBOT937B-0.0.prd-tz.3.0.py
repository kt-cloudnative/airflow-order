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
                , WORKFLOW_NAME='IA_CBOT937B',WORKFLOW_ID='285c558a75d849e8914f01fecefeda41', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT937B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('285c558a75d849e8914f01fecefeda41')

    cbot937bJob_vol = []
    cbot937bJob_volMnt = []
    cbot937bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot937bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot937bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot937bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2c4330bb108742c8ab365ef802df6694',
        'volumes': cbot937bJob_vol,
        'volume_mounts': cbot937bJob_volMnt,
        'env_from':cbot937bJob_env,
        'task_id':'cbot937bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.7.1.5',
        'arguments':["--job.name=cbot937bJob", "requestDate="+str(datetime.now()), "procDate=20241129", "argc=2"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('285c558a75d849e8914f01fecefeda41')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot937bJob,
        Complete
    ]) 

    # authCheck >> cbot937bJob >> Complete
    workflow








