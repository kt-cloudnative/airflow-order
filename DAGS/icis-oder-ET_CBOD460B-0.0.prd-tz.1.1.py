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
                , WORKFLOW_NAME='ET_CBOD460B',WORKFLOW_ID='a431edb984094985b51bda71682fa670', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOD460B-0.0.prd-tz.1.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 1, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a431edb984094985b51bda71682fa670')

    cbod460bJob_vol = []
    cbod460bJob_volMnt = []
    cbod460bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod460bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod460bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbod460bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbod460bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod460bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '78194106f09546c589120a415b23d272',
        'volumes': cbod460bJob_vol,
        'volume_mounts': cbod460bJob_volMnt,
        'env_from':cbod460bJob_env,
        'task_id':'cbod460bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.7',
        'arguments':["--job.name=cbod460bJob", "tranDate=${YYYYMM}", "logLevel=debug", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a431edb984094985b51bda71682fa670')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod460bJob,
        Complete
    ]) 

    # authCheck >> cbod460bJob >> Complete
    workflow








