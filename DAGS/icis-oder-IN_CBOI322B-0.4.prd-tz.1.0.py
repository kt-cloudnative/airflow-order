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
                , WORKFLOW_NAME='IN_CBOI322B',WORKFLOW_ID='8d7c5728043f46a3adb1e8a9b8de5f99', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI322B-0.4.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 10, 17, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8d7c5728043f46a3adb1e8a9b8de5f99')

    cboi322bJob_vol = []
    cboi322bJob_volMnt = []
    cboi322bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboi322bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboi322bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cboi322bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cboi322bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi322bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f80f4aba32c44fd6a89e63cf3712b245',
        'volumes': cboi322bJob_vol,
        'volume_mounts': cboi322bJob_volMnt,
        'env_from':cboi322bJob_env,
        'task_id':'cboi322bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.2',
        'arguments':["--job.name=cboi322bJob", "endTranDate=20240816", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8d7c5728043f46a3adb1e8a9b8de5f99')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi322bJob,
        Complete
    ]) 

    # authCheck >> cboi322bJob >> Complete
    workflow








