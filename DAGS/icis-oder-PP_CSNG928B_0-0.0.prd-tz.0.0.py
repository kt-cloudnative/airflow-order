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
                , WORKFLOW_NAME='PP_CSNG928B_0',WORKFLOW_ID='6ff66d91755a4a70b0c54c85690abcf2', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG928B_0-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 4, 5, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6ff66d91755a4a70b0c54c85690abcf2')

    csng928bJob_vol = []
    csng928bJob_volMnt = []
    csng928bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng928bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng928bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng928bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6fbf2db7553e4242b3e8a04574fc7610',
        'volumes': csng928bJob_vol,
        'volume_mounts': csng928bJob_volMnt,
        'env_from':csng928bJob_env,
        'task_id':'csng928bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng928bJob", "duplPrvnDate=${YYYYMMDDHHMISSSSS}", "trtDate=${YYYYMMDD}", "subStrSaId=0"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6ff66d91755a4a70b0c54c85690abcf2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng928bJob,
        Complete
    ]) 

    # authCheck >> csng928bJob >> Complete
    workflow








