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
                , WORKFLOW_NAME='PP_CBON313B',WORKFLOW_ID='4e21dce11b044a78b1421490d493711f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON313B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4e21dce11b044a78b1421490d493711f')

    cbon313bJob_vol = []
    cbon313bJob_volMnt = []
    cbon313bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon313bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon313bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbon313bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon313bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon313bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '152d7d8f32cb46359fab952195ab5a46',
        'volumes': cbon313bJob_vol,
        'volume_mounts': cbon313bJob_volMnt,
        'env_from':cbon313bJob_env,
        'task_id':'cbon313bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=cbon313bJob","endTranDate=20240628","requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4e21dce11b044a78b1421490d493711f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon313bJob,
        Complete
    ]) 

    # authCheck >> cbon313bJob >> Complete
    workflow








