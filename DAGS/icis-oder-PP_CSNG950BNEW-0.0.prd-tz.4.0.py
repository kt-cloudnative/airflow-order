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
                , WORKFLOW_NAME='PP_CSNG950BNEW',WORKFLOW_ID='21c4fab9ce4348619fd7eaa4e8f2c7ad', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG950BNEW-0.0.prd-tz.4.0'
    ,'schedule_interval':'00 01 * * *'
    ,'start_date': datetime(2024, 8, 25, 1, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('21c4fab9ce4348619fd7eaa4e8f2c7ad')

    csng950bJob_vol = []
    csng950bJob_volMnt = []
    csng950bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng950bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng950bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng950bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '50dbda4c607c4b8ba4b95cb1920ba0f5',
        'volumes': csng950bJob_vol,
        'volume_mounts': csng950bJob_volMnt,
        'env_from':csng950bJob_env,
        'task_id':'csng950bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng950bJob","workDate=${YYYYMMDD}", "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('21c4fab9ce4348619fd7eaa4e8f2c7ad')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng950bJob,
        Complete
    ]) 

    # authCheck >> csng950bJob >> Complete
    workflow








