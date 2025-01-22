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
                , WORKFLOW_NAME='PP_CBOG829B',WORKFLOW_ID='37bb08a305784b8e8c6b78cd4ce3c469', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG829B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('37bb08a305784b8e8c6b78cd4ce3c469')

    cbog829bJob_vol = []
    cbog829bJob_volMnt = []
    cbog829bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog829bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog829bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '12f0dc7d6d0446059a6a16542f769920',
        'volumes': cbog829bJob_vol,
        'volume_mounts': cbog829bJob_volMnt,
        'env_from':cbog829bJob_env,
        'task_id':'cbog829bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.76',
        'arguments':["--job.name=cbog829bJob" , "procNo=3","requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('37bb08a305784b8e8c6b78cd4ce3c469')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog829bJob,
        Complete
    ]) 

    # authCheck >> cbog829bJob >> Complete
    workflow








