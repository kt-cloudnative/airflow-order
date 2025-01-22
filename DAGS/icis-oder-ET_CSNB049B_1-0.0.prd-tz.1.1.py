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
                , WORKFLOW_NAME='ET_CSNB049B_1',WORKFLOW_ID='1fa272f84f95430e8dbd40c7d9a9b5f8', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CSNB049B_1-0.0.prd-tz.1.1'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 10, 10, 57, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1fa272f84f95430e8dbd40c7d9a9b5f8')

    csnb049bJob_vol = []
    csnb049bJob_volMnt = []
    csnb049bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnb049bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    csnb049bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnb049bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3114e8691d1b4a6aae9a55eaf609cb0b',
        'volumes': csnb049bJob_vol,
        'volume_mounts': csnb049bJob_volMnt,
        'env_from':csnb049bJob_env,
        'task_id':'csnb049bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.7',
        'arguments':["--job.name=csnb049bJob", "workYm=${YYYYMM}", "workGb=1", "regEmpNo=91108904", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1fa272f84f95430e8dbd40c7d9a9b5f8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnb049bJob,
        Complete
    ]) 

    # authCheck >> csnb049bJob >> Complete
    workflow








