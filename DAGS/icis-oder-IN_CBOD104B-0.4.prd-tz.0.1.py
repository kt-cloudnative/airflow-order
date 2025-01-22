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
                , WORKFLOW_NAME='IN_CBOD104B',WORKFLOW_ID='bd49c80253fd4260bd43d6f16127076e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOD104B-0.4.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 10, 17, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bd49c80253fd4260bd43d6f16127076e')

    cbod104bJob_vol = []
    cbod104bJob_volMnt = []
    cbod104bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod104bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod104bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cbod104bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod104bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod104bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2e2d72cd6e464bd78d8e30a444f072ee',
        'volumes': cbod104bJob_vol,
        'volume_mounts': cbod104bJob_volMnt,
        'env_from':cbod104bJob_env,
        'task_id':'cbod104bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.1',
        'arguments':["--job.name=cbod104bJob",  "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bd49c80253fd4260bd43d6f16127076e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod104bJob,
        Complete
    ]) 

    # authCheck >> cbod104bJob >> Complete
    workflow








