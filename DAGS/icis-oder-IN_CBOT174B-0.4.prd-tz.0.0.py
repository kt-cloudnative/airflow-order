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
                , WORKFLOW_NAME='IN_CBOT174B',WORKFLOW_ID='404292cc83f54fda9317e4a3bb8a7480', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOT174B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 2, 13, 36, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('404292cc83f54fda9317e4a3bb8a7480')

    cbot174bJob_vol = []
    cbot174bJob_volMnt = []
    cbot174bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot174bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot174bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot174bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cbot174bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot174bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ebfd0c7dfa0a4649a9221fcbaa9b8514',
        'volumes': cbot174bJob_vol,
        'volume_mounts': cbot174bJob_volMnt,
        'env_from':cbot174bJob_env,
        'task_id':'cbot174bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.6',
        'arguments':["--job.name=cbot174bJob", "startDate=${YYYYMM}01", "closeDate=${YYYYMMDD}", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('404292cc83f54fda9317e4a3bb8a7480')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot174bJob,
        Complete
    ]) 

    # authCheck >> cbot174bJob >> Complete
    workflow








