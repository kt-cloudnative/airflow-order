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
                , WORKFLOW_NAME='IN_CBOT361B',WORKFLOW_ID='a26d29fffefe4e32b2e74203535f6ae4', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOT361B-0.4.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a26d29fffefe4e32b2e74203535f6ae4')

    cbot361bJob_vol = []
    cbot361bJob_volMnt = []
    cbot361bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot361bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot361bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cbot361bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot361bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot361bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '107ea05dfcba42d083856b957f04c6b3',
        'volumes': cbot361bJob_vol,
        'volume_mounts': cbot361bJob_volMnt,
        'env_from':cbot361bJob_env,
        'task_id':'cbot361bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.1',
        'arguments':["--job.name=cbot361bJob", "closeDate=${YYYYMMDD}", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a26d29fffefe4e32b2e74203535f6ae4')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot361bJob,
        Complete
    ]) 

    # authCheck >> cbot361bJob >> Complete
    workflow








