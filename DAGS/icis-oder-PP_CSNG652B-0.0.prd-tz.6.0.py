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
                , WORKFLOW_NAME='PP_CSNG652B',WORKFLOW_ID='f58d7ca4956c47ae9518c99da9c76415', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG652B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 11, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f58d7ca4956c47ae9518c99da9c76415')

    csng652bJob_vol = []
    csng652bJob_volMnt = []
    csng652bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng652bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng652bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng652bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b9633da78faf4f429dbbd9db75192f78',
        'volumes': csng652bJob_vol,
        'volume_mounts': csng652bJob_volMnt,
        'env_from':csng652bJob_env,
        'task_id':'csng652bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.144',
        'arguments':["--job.name=csng652bJob","fromDate=20250108", "toDate=20250109", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f58d7ca4956c47ae9518c99da9c76415')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng652bJob,
        Complete
    ]) 

    # authCheck >> csng652bJob >> Complete
    workflow








