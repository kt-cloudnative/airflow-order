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
                , WORKFLOW_NAME='IN_CBOD104B',WORKFLOW_ID='c7f74f0e1aa94561aa323638f9620f0e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOD104B-0.4.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 1, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c7f74f0e1aa94561aa323638f9620f0e')

    cbod104bJob_vol = []
    cbod104bJob_volMnt = []
    cbod104bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbod104bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbod104bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbod104bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cbod104bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod104bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a976f083eabb496282fec9aa9d7e7202',
        'volumes': cbod104bJob_vol,
        'volume_mounts': cbod104bJob_volMnt,
        'env_from':cbod104bJob_env,
        'task_id':'cbod104bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.7',
        'arguments':["--job.name=cbod104bJob", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c7f74f0e1aa94561aa323638f9620f0e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod104bJob,
        Complete
    ]) 

    # authCheck >> cbod104bJob >> Complete
    workflow








