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
                , WORKFLOW_NAME='IN_CBOI329B',WORKFLOW_ID='8a73a32ae33842afb87d857fc5a51afc', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI329B-0.4.prd-tz.0.1'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 6, 8, 52, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8a73a32ae33842afb87d857fc5a51afc')

    cboi329bJob_vol = []
    cboi329bJob_volMnt = []
    cboi329bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboi329bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboi329bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboi329bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cboi329bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi329bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f253a9dad9a64aedbf8521c7de543652',
        'volumes': cboi329bJob_vol,
        'volume_mounts': cboi329bJob_volMnt,
        'env_from':cboi329bJob_env,
        'task_id':'cboi329bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.6',
        'arguments':["--job.name=cboi329bJob", "rcvEndDate=20241129", "regerEmpOfc=710571", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8a73a32ae33842afb87d857fc5a51afc')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi329bJob,
        Complete
    ]) 

    # authCheck >> cboi329bJob >> Complete
    workflow








