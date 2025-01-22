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
                , WORKFLOW_NAME='PP_CSNG133B',WORKFLOW_ID='3938625341fc4343a17807692e6b0318', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG133B-0.0.prd-tz.4.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 11, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3938625341fc4343a17807692e6b0318')

    csng133bJob_vol = []
    csng133bJob_volMnt = []
    csng133bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng133bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng133bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng133bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng133bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng133bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '87f5abaad9874afb92dc9c24ccae06d3',
        'volumes': csng133bJob_vol,
        'volume_mounts': csng133bJob_volMnt,
        'env_from':csng133bJob_env,
        'task_id':'csng133bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng133bJob", "requestDate=${YYYYMMDDHHMISSSSS}"
, "workMonth=202411"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3938625341fc4343a17807692e6b0318')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng133bJob,
        Complete
    ]) 

    # authCheck >> csng133bJob >> Complete
    workflow








