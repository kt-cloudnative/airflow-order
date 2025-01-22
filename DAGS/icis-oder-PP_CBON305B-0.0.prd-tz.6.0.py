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
                , WORKFLOW_NAME='PP_CBON305B',WORKFLOW_ID='e194ff67606d46db9f4d4a47096a3f98', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON305B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 12, 9, 21, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e194ff67606d46db9f4d4a47096a3f98')

    cbon305bJob_vol = []
    cbon305bJob_volMnt = []
    cbon305bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon305bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon305bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbon305bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbon305bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon305bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '85c5483f81f44660a943af9fd261432e',
        'volumes': cbon305bJob_vol,
        'volume_mounts': cbon305bJob_volMnt,
        'env_from':cbon305bJob_env,
        'task_id':'cbon305bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=cbon305bJob", "endTranDate=${YYYYMMDD}", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e194ff67606d46db9f4d4a47096a3f98')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon305bJob,
        Complete
    ]) 

    # authCheck >> cbon305bJob >> Complete
    workflow








