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
                , WORKFLOW_NAME='PP_CSNG950BNEW',WORKFLOW_ID='55d0f0277de24f62b8ee4dfe0046aa2e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG950BNEW-0.0.prd-tz.5.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('55d0f0277de24f62b8ee4dfe0046aa2e')

    csng950bJob_vol = []
    csng950bJob_volMnt = []
    csng950bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng950bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng950bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng950bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng950bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng950bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4e04b7710f0946e0995aed64a29503f3',
        'volumes': csng950bJob_vol,
        'volume_mounts': csng950bJob_volMnt,
        'env_from':csng950bJob_env,
        'task_id':'csng950bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng950bJob","workDate=20241129", "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('55d0f0277de24f62b8ee4dfe0046aa2e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng950bJob,
        Complete
    ]) 

    # authCheck >> csng950bJob >> Complete
    workflow








