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
                , WORKFLOW_NAME='IN_CBOT361B',WORKFLOW_ID='8bb9b8064cb74bffb09a04c8934dc0a1', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOT361B-0.4.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 10, 17, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8bb9b8064cb74bffb09a04c8934dc0a1')

    cbot361bJob_vol = []
    cbot361bJob_volMnt = []
    cbot361bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot361bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot361bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cbot361bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot361bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot361bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1a009adaea14446abaeb6a48df07f63d',
        'volumes': cbot361bJob_vol,
        'volume_mounts': cbot361bJob_volMnt,
        'env_from':cbot361bJob_env,
        'task_id':'cbot361bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.2',
        'arguments':["--job.name=cbot361bJob", "closeDate=20240731", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8bb9b8064cb74bffb09a04c8934dc0a1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot361bJob,
        Complete
    ]) 

    # authCheck >> cbot361bJob >> Complete
    workflow








