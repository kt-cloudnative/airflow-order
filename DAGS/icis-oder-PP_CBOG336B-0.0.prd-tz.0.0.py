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
                , WORKFLOW_NAME='PP_CBOG336B',WORKFLOW_ID='e685a48e2bc943caabaf6f1c6f0b8c6a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG336B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 18, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e685a48e2bc943caabaf6f1c6f0b8c6a')

    cbog336bJob_vol = []
    cbog336bJob_volMnt = []
    cbog336bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog336bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog336bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog336bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog336bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog336bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '368a4f99f73e4bc993bb4836db014713',
        'volumes': cbog336bJob_vol,
        'volume_mounts': cbog336bJob_volMnt,
        'env_from':cbog336bJob_env,
        'task_id':'cbog336bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=cbog336bJob", "endTranDate=202412", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e685a48e2bc943caabaf6f1c6f0b8c6a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog336bJob,
        Complete
    ]) 

    # authCheck >> cbog336bJob >> Complete
    workflow








