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
                , WORKFLOW_NAME='ET_CSNG918B_5',WORKFLOW_ID='f48a216a29ad4ff2b30d8e301a3f9458', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CSNG918B_5-0.0.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 10, 10, 50, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f48a216a29ad4ff2b30d8e301a3f9458')

    csng918bJob_vol = []
    csng918bJob_volMnt = []
    csng918bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng918bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng918bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng918bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    csng918bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng918bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '73f5aab4671f4a958e06a7a3daa63db5',
        'volumes': csng918bJob_vol,
        'volume_mounts': csng918bJob_volMnt,
        'env_from':csng918bJob_env,
        'task_id':'csng918bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.4.1.65',
        'arguments':["--job.name=csng918bJob", "invMonth=${YYYYMM}", "subSaId=5", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f48a216a29ad4ff2b30d8e301a3f9458')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng918bJob,
        Complete
    ]) 

    # authCheck >> csng918bJob >> Complete
    workflow








