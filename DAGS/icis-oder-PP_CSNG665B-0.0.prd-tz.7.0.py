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
                , WORKFLOW_NAME='PP_CSNG665B',WORKFLOW_ID='e310da41ff074d6b99d46e91f710cadb', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG665B-0.0.prd-tz.7.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e310da41ff074d6b99d46e91f710cadb')

    csng665bJob_vol = []
    csng665bJob_volMnt = []
    csng665bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng665bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng665bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng665bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng665bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng665bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '994d0ca252824cbb8bef9a46b08d04c9',
        'volumes': csng665bJob_vol,
        'volume_mounts': csng665bJob_volMnt,
        'env_from':csng665bJob_env,
        'task_id':'csng665bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng665bJob", "fromDate=20241101", "toDate=20241129", "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e310da41ff074d6b99d46e91f710cadb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng665bJob,
        Complete
    ]) 

    # authCheck >> csng665bJob >> Complete
    workflow








