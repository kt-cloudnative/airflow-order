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
                , WORKFLOW_NAME='IN_CBOI322B',WORKFLOW_ID='8b3c4b84d6d84ecbba597495feb5d93f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI322B-0.4.prd-tz.3.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 1, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8b3c4b84d6d84ecbba597495feb5d93f')

    cboi322bJob_vol = []
    cboi322bJob_volMnt = []
    cboi322bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboi322bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboi322bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboi322bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cboi322bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi322bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '25d609ef870147aea219b74748683a54',
        'volumes': cboi322bJob_vol,
        'volume_mounts': cboi322bJob_volMnt,
        'env_from':cboi322bJob_env,
        'task_id':'cboi322bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.7',
        'arguments':["--job.name=cboi322bJob", "endTranDate=${YYYYMMDD}", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8b3c4b84d6d84ecbba597495feb5d93f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi322bJob,
        Complete
    ]) 

    # authCheck >> cboi322bJob >> Complete
    workflow








