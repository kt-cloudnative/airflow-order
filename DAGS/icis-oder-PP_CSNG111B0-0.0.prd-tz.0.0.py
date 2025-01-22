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
                , WORKFLOW_NAME='PP_CSNG111B0',WORKFLOW_ID='3d42f39c850a4933843d246ca5d76429', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG111B0-0.0.prd-tz.0.0'
    ,'schedule_interval':'00 02 * * *'
    ,'start_date': datetime(2024, 8, 25, 2, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3d42f39c850a4933843d246ca5d76429')

    csng111bJob_vol = []
    csng111bJob_volMnt = []
    csng111bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng111bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng111bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng111bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '39911ff5224a4fbb8853a50477a3040d',
        'volumes': csng111bJob_vol,
        'volume_mounts': csng111bJob_volMnt,
        'env_from':csng111bJob_env,
        'task_id':'csng111bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng111bJob", "empNo=csng111b", "workDate=${YYYYMMDD}", "subSaId=0", "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3d42f39c850a4933843d246ca5d76429')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng111bJob,
        Complete
    ]) 

    # authCheck >> csng111bJob >> Complete
    workflow








