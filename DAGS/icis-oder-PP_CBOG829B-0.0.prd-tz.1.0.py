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
                , WORKFLOW_NAME='PP_CBOG829B',WORKFLOW_ID='a3148b820bf8412fb076d62458feb310', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG829B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 40, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a3148b820bf8412fb076d62458feb310')

    cbog829bJob_vol = []
    cbog829bJob_volMnt = []
    cbog829bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog829bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog829bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a4315c58319e470a86356d5a6aba9455',
        'volumes': cbog829bJob_vol,
        'volume_mounts': cbog829bJob_volMnt,
        'env_from':cbog829bJob_env,
        'task_id':'cbog829bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog829bJob" , "procNo=3","requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a3148b820bf8412fb076d62458feb310')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog829bJob,
        Complete
    ]) 

    # authCheck >> cbog829bJob >> Complete
    workflow








