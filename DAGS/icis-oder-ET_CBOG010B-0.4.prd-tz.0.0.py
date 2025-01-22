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
                , WORKFLOW_NAME='ET_CBOG010B',WORKFLOW_ID='95ac7c38d61d4a11b42e8a9e993b4334', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOG010B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2024, 12, 30, 15, 27, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('95ac7c38d61d4a11b42e8a9e993b4334')

    cbog010bJob_vol = []
    cbog010bJob_volMnt = []
    cbog010bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog010bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog010bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog010bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbog010bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog010bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3a4c46165b7045c39efc1b662ee5d055',
        'volumes': cbog010bJob_vol,
        'volume_mounts': cbog010bJob_volMnt,
        'env_from':cbog010bJob_env,
        'task_id':'cbog010bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.4.1.65',
        'arguments':["--job.name=cbog010bJob", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('95ac7c38d61d4a11b42e8a9e993b4334')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog010bJob,
        Complete
    ]) 

    # authCheck >> cbog010bJob >> Complete
    workflow








