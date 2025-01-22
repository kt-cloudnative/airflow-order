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
                , WORKFLOW_NAME='PP_CSNG937B',WORKFLOW_ID='5a8a3a5ddc9543629d1537e27d2d245d', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG937B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 19, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('5a8a3a5ddc9543629d1537e27d2d245d')

    csng937bJob_vol = []
    csng937bJob_volMnt = []
    csng937bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng937bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng937bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng937bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng937bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng937bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a85d44f6f6c5499e8f27a2aa908dd81c',
        'volumes': csng937bJob_vol,
        'volume_mounts': csng937bJob_volMnt,
        'env_from':csng937bJob_env,
        'task_id':'csng937bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.127',
        'arguments':["--job.name=csng937bJob", "workDate=${YYYYMMDD}", "duplExePram=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('5a8a3a5ddc9543629d1537e27d2d245d')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng937bJob,
        Complete
    ]) 

    # authCheck >> csng937bJob >> Complete
    workflow








