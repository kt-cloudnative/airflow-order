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
                , WORKFLOW_NAME='PP_CBOD202B',WORKFLOW_ID='4ba3b82614ef4a29aa0e357389b455fb', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD202B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 5, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4ba3b82614ef4a29aa0e357389b455fb')

    cbod202bJob_vol = []
    cbod202bJob_volMnt = []
    cbod202bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbod202bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbod202bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod202bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6f5cb071d6fd40e994efd59860b1a569',
        'volumes': cbod202bJob_vol,
        'volume_mounts': cbod202bJob_volMnt,
        'env_from':cbod202bJob_env,
        'task_id':'cbod202bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=cbod202bJob", "endTranDate=${YYYYMMDD}", "fromTime=${HH}", "toTime=${HH}", "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4ba3b82614ef4a29aa0e357389b455fb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod202bJob,
        Complete
    ]) 

    # authCheck >> cbod202bJob >> Complete
    workflow








