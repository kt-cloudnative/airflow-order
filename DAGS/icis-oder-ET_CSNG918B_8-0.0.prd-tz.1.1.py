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
                , WORKFLOW_NAME='ET_CSNG918B_8',WORKFLOW_ID='a01d9546be38459aa7bab12df46b880a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CSNG918B_8-0.0.prd-tz.1.1'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 10, 10, 53, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a01d9546be38459aa7bab12df46b880a')

    csng918bJob_vol = []
    csng918bJob_volMnt = []
    csng918bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng918bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    csng918bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng918bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8451b5c8936246e98c1c6af13485b22d',
        'volumes': csng918bJob_vol,
        'volume_mounts': csng918bJob_volMnt,
        'env_from':csng918bJob_env,
        'task_id':'csng918bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.7',
        'arguments':["--job.name=csng918bJob", "invMonth=${YYYYMM}", "subSaId=8", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a01d9546be38459aa7bab12df46b880a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng918bJob,
        Complete
    ]) 

    # authCheck >> csng918bJob >> Complete
    workflow








