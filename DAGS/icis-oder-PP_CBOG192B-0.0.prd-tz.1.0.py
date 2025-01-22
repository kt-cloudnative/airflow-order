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
                , WORKFLOW_NAME='PP_CBOG192B',WORKFLOW_ID='04ed8f8d0d5c49409eb182987a529be4', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG192B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('04ed8f8d0d5c49409eb182987a529be4')

    cbog192bJob_vol = []
    cbog192bJob_volMnt = []
    cbog192bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog192bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog192bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog192bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog192bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog192bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '421f6306064c4b6a83fce904865b5de9',
        'volumes': cbog192bJob_vol,
        'volume_mounts': cbog192bJob_volMnt,
        'env_from':cbog192bJob_env,
        'task_id':'cbog192bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog192bJob", "requestDate=${YYYYMMDDHHMISSSSS}"
, "procDate=20241129","sinaeCd=1","procGb=2","fileName=KT20241224"
, "svcName=cbog192b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('04ed8f8d0d5c49409eb182987a529be4')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog192bJob,
        Complete
    ]) 

    # authCheck >> cbog192bJob >> Complete
    workflow








