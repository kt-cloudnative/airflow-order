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
                , WORKFLOW_NAME='IN_CBOL831B',WORKFLOW_ID='69e46edd228b480387988b508ab82632', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOL831B-0.4.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 10, 18, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('69e46edd228b480387988b508ab82632')

    cbol831bJob_vol = []
    cbol831bJob_volMnt = []
    cbol831bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbol831bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbol831bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cbol831bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbol831bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol831bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '37c0c0f7e85846229c0cca9b23e93686',
        'volumes': cbol831bJob_vol,
        'volume_mounts': cbol831bJob_volMnt,
        'env_from':cbol831bJob_env,
        'task_id':'cbol831bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.2',
        'arguments':["--job.name=cbol831bJob",  "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('69e46edd228b480387988b508ab82632')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol831bJob,
        Complete
    ]) 

    # authCheck >> cbol831bJob >> Complete
    workflow








