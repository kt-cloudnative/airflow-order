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
                , WORKFLOW_NAME='PP_CBOE016B',WORKFLOW_ID='491ec762f0354a0bb47410a395ab1ca5', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOE016B-0.0.prd-tz.4.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('491ec762f0354a0bb47410a395ab1ca5')

    cboe016bJob_vol = []
    cboe016bJob_volMnt = []
    cboe016bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboe016bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboe016bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboe016bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cboe016bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboe016bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'cc6cb9b12aaa4fdca6281886cb99b945',
        'volumes': cboe016bJob_vol,
        'volume_mounts': cboe016bJob_volMnt,
        'env_from':cboe016bJob_env,
        'task_id':'cboe016bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cboe016bJob"
, "requestDate=${YYYYMMDDHHMISSSSS}"
, "yyyyMm=202411", "svcName=cboe016bJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('491ec762f0354a0bb47410a395ab1ca5')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboe016bJob,
        Complete
    ]) 

    # authCheck >> cboe016bJob >> Complete
    workflow








