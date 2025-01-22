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
                , WORKFLOW_NAME='PP_CSNC130B',WORKFLOW_ID='bc51c52173af4ec183e26d7e4c8bb156', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNC130B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bc51c52173af4ec183e26d7e4c8bb156')

    csnc130bJob_vol = []
    csnc130bJob_volMnt = []
    csnc130bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc130bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc130bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc130bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc130bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc130bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9043644d882849f68fb55fa1bde522aa',
        'volumes': csnc130bJob_vol,
        'volume_mounts': csnc130bJob_volMnt,
        'env_from':csnc130bJob_env,
        'task_id':'csnc130bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.24',
        'arguments':["--job.name=csnc130bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bc51c52173af4ec183e26d7e4c8bb156')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnc130bJob,
        Complete
    ]) 

    # authCheck >> csnc130bJob >> Complete
    workflow








