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
                , WORKFLOW_NAME='IN_CBOT183B',WORKFLOW_ID='083001601dc74d6b9a8acae7106a3f9e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOT183B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 6, 17, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('083001601dc74d6b9a8acae7106a3f9e')

    cbot183bJob_vol = []
    cbot183bJob_volMnt = []
    cbot183bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot183bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot183bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot183bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cbot183bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot183bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '541b2a40b9224eb08547dfd33c96ea79',
        'volumes': cbot183bJob_vol,
        'volume_mounts': cbot183bJob_volMnt,
        'env_from':cbot183bJob_env,
        'task_id':'cbot183bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.4.0.78',
        'arguments':["--job.name=cbot183bJob", "lobCd=IN", "inDate=${YYYYMMDD, MM, -1}", "params=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('083001601dc74d6b9a8acae7106a3f9e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot183bJob,
        Complete
    ]) 

    # authCheck >> cbot183bJob >> Complete
    workflow








