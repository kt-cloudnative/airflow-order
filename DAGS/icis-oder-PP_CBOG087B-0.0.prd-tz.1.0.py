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
                , WORKFLOW_NAME='PP_CBOG087B',WORKFLOW_ID='9644805fd4eb46d89ca09dfe1fc2ac91', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG087B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 11, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('9644805fd4eb46d89ca09dfe1fc2ac91')

    cbog087bJob_vol = []
    cbog087bJob_volMnt = []
    cbog087bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog087bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog087bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog087bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog087bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog087bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '629589c42d4d44ee88356a51d7006a68',
        'volumes': cbog087bJob_vol,
        'volume_mounts': cbog087bJob_volMnt,
        'env_from':cbog087bJob_env,
        'task_id':'cbog087bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog087bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('9644805fd4eb46d89ca09dfe1fc2ac91')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog087bJob,
        Complete
    ]) 

    # authCheck >> cbog087bJob >> Complete
    workflow








