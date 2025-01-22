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
                , WORKFLOW_NAME='PP_CSNG133B',WORKFLOW_ID='d9d4654eff174927aa7c586141a30c94', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG133B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 18, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('d9d4654eff174927aa7c586141a30c94')

    csng133bJob_vol = []
    csng133bJob_volMnt = []
    csng133bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng133bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng133bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng133bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng133bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng133bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '02a1195fb685446c85ae3f6545d1b11a',
        'volumes': csng133bJob_vol,
        'volume_mounts': csng133bJob_volMnt,
        'env_from':csng133bJob_env,
        'task_id':'csng133bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.56',
        'arguments':["--job.name=csng133bJob", "requestDate=${YYYYMMDDHHMISSSSS}"
, "workMonth=${YYYYMM}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('d9d4654eff174927aa7c586141a30c94')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng133bJob,
        Complete
    ]) 

    # authCheck >> csng133bJob >> Complete
    workflow








