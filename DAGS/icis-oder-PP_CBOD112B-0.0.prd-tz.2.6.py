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
                , WORKFLOW_NAME='PP_CBOD112B',WORKFLOW_ID='01c3637e620440d3933ac57d0e2f32c0', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD112B-0.0.prd-tz.2.6'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('01c3637e620440d3933ac57d0e2f32c0')

    cbod112bJob_vol = []
    cbod112bJob_volMnt = []
    cbod112bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod112bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod112bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbod112bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod112bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod112bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e785a5b79d0242be835d73db18c7ae42',
        'volumes': cbod112bJob_vol,
        'volume_mounts': cbod112bJob_volMnt,
        'env_from':cbod112bJob_env,
        'task_id':'cbod112bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=cbod112bJob", "endTranDate=20240430", "fromTime=00","toTime=13" ,"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('01c3637e620440d3933ac57d0e2f32c0')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod112bJob,
        Complete
    ]) 

    # authCheck >> cbod112bJob >> Complete
    workflow








