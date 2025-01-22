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
                , WORKFLOW_NAME='PP_CBOG998B',WORKFLOW_ID='f56eec2b416549fab63b4ed386c820bc', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG998B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 40, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f56eec2b416549fab63b4ed386c820bc')

    cbog998bJob_vol = []
    cbog998bJob_volMnt = []
    cbog998bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog998bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog998bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog998bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog998bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog998bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'fec956c4b93d45f19a2ce16e0d8c9068',
        'volumes': cbog998bJob_vol,
        'volume_mounts': cbog998bJob_volMnt,
        'env_from':cbog998bJob_env,
        'task_id':'cbog998bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog998bJob", "procMm=202411", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f56eec2b416549fab63b4ed386c820bc')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog998bJob,
        Complete
    ]) 

    # authCheck >> cbog998bJob >> Complete
    workflow








