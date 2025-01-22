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
                , WORKFLOW_NAME='IN_CBOI318B',WORKFLOW_ID='e962e837948b4ac894482b95b491ec8c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI318B-0.4.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 10, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e962e837948b4ac894482b95b491ec8c')

    cboi318bJob_vol = []
    cboi318bJob_volMnt = []
    cboi318bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboi318bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboi318bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cboi318bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cboi318bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi318bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4028c91346a945ebb8bb4a4ab5361514',
        'volumes': cboi318bJob_vol,
        'volume_mounts': cboi318bJob_volMnt,
        'env_from':cboi318bJob_env,
        'task_id':'cboi318bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.2',
        'arguments':["--job.name=cboi318bJob", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e962e837948b4ac894482b95b491ec8c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi318bJob,
        Complete
    ]) 

    # authCheck >> cboi318bJob >> Complete
    workflow








