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
                , WORKFLOW_NAME='ET_CBOG050B',WORKFLOW_ID='0935aa4f44a94cc091f492cfd551ed5a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOG050B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 7, 17, 14, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0935aa4f44a94cc091f492cfd551ed5a')

    cbog050bJob_vol = []
    cbog050bJob_volMnt = []
    cbog050bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog050bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog050bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog050bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbog050bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog050bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9a0cfd05a28546178173c7708cccb97d',
        'volumes': cbog050bJob_vol,
        'volume_mounts': cbog050bJob_volMnt,
        'env_from':cbog050bJob_env,
        'task_id':'cbog050bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.4.1.64',
        'arguments':["--job.name=cbog050bJob" , "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0935aa4f44a94cc091f492cfd551ed5a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog050bJob,
        Complete
    ]) 

    # authCheck >> cbog050bJob >> Complete
    workflow








