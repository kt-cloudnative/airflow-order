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
                , WORKFLOW_NAME='PP_CBOD106B',WORKFLOW_ID='a9c895dfb5b342b8874cf261f10920bf', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD106B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 18, 48, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a9c895dfb5b342b8874cf261f10920bf')

    cbod106bJob_vol = []
    cbod106bJob_volMnt = []
    cbod106bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbod106bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod106bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod106bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '005dd33d1f284ee4ae93dd4c3938b401',
        'volumes': cbod106bJob_vol,
        'volume_mounts': cbod106bJob_volMnt,
        'env_from':cbod106bJob_env,
        'task_id':'cbod106bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.51',
        'arguments':["--job.name=cbod106bJob", "workDate=20240731", "saveSaId=24994981504","saveOrdNo=24268GH21545893", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a9c895dfb5b342b8874cf261f10920bf')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod106bJob,
        Complete
    ]) 

    # authCheck >> cbod106bJob >> Complete
    workflow








