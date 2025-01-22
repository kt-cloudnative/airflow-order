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
                , WORKFLOW_NAME='PP_CBOG104B',WORKFLOW_ID='3620bb32bd394a648cd3a2e43f5d2c7e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG104B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 18, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3620bb32bd394a648cd3a2e43f5d2c7e')

    cbog104bJob_vol = []
    cbog104bJob_volMnt = []
    cbog104bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog104bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog104bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog104bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog104bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog104bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ad78d0acb7cf47fcb00881e20eb7a66f',
        'volumes': cbog104bJob_vol,
        'volume_mounts': cbog104bJob_volMnt,
        'env_from':cbog104bJob_env,
        'task_id':'cbog104bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=cbog104bJob", "rsName=KK", "requestDate="+str(datetime.now().strftime("${YYYYMMDDHHMISSSSS}"))],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3620bb32bd394a648cd3a2e43f5d2c7e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog104bJob,
        Complete
    ]) 

    # authCheck >> cbog104bJob >> Complete
    workflow








