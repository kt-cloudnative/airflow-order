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
                , WORKFLOW_NAME='IN_CBOI329B',WORKFLOW_ID='7956d089228b4450a74f616162fe4dad', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI329B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 6, 8, 52, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7956d089228b4450a74f616162fe4dad')

    cboi329bJob_vol = []
    cboi329bJob_volMnt = []
    cboi329bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboi329bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboi329bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboi329bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cboi329bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi329bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b3061e74e49f43f5bbd5d8704cdd949a',
        'volumes': cboi329bJob_vol,
        'volume_mounts': cboi329bJob_volMnt,
        'env_from':cboi329bJob_env,
        'task_id':'cboi329bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.6',
        'arguments':["--job.name=cboi329bJob", "rcvEndDate=${YYYYMMDD}", "regerEmpOfc=710571", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7956d089228b4450a74f616162fe4dad')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi329bJob,
        Complete
    ]) 

    # authCheck >> cboi329bJob >> Complete
    workflow








