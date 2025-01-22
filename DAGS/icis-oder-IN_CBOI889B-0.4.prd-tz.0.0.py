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
                , WORKFLOW_NAME='IN_CBOI889B',WORKFLOW_ID='30bb6e68dacd4b94bae6fdd908f0fbe8', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI889B-0.4.prd-tz.0.0'
    ,'schedule_interval':'0 21 * * *'
    ,'start_date': datetime(2025, 1, 1, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('30bb6e68dacd4b94bae6fdd908f0fbe8')

    cboi889bJob_vol = []
    cboi889bJob_volMnt = []
    cboi889bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cboi889bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cboi889bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboi889bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cboi889bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi889bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '89320bc7a9b044989014ebb2206f8202',
        'volumes': cboi889bJob_vol,
        'volume_mounts': cboi889bJob_volMnt,
        'env_from':cboi889bJob_env,
        'task_id':'cboi889bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.4.1.65',
        'arguments':["--job.name=cboi889bJob", "count=1000", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('30bb6e68dacd4b94bae6fdd908f0fbe8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi889bJob,
        Complete
    ]) 

    # authCheck >> cboi889bJob >> Complete
    workflow








