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
                , WORKFLOW_NAME='PP_CSNG212B',WORKFLOW_ID='3bdc74d5c5e9409380759b204c5823a7', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG212B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 7, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3bdc74d5c5e9409380759b204c5823a7')

    csng212bJob_vol = []
    csng212bJob_volMnt = []
    csng212bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng212bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng212bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng212bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '743bc866d74c40a98eaf2e57172c9f6b',
        'volumes': csng212bJob_vol,
        'volume_mounts': csng212bJob_volMnt,
        'env_from':csng212bJob_env,
        'task_id':'csng212bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.56',
        'arguments':["--job.name=csng212bJob", "workMonth=202407", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3bdc74d5c5e9409380759b204c5823a7')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng212bJob,
        Complete
    ]) 

    # authCheck >> csng212bJob >> Complete
    workflow








