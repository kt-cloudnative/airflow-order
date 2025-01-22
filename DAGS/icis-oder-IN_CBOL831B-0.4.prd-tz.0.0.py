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
                , WORKFLOW_NAME='IN_CBOL831B',WORKFLOW_ID='362a9b9718e34979933ca657df59cd7c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOL831B-0.4.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('362a9b9718e34979933ca657df59cd7c')

    cbol831bJob_vol = []
    cbol831bJob_volMnt = []
    cbol831bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbol831bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbol831bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cbol831bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbol831bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol831bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '67eb1854c2034c1fbf2f040d60cd5d42',
        'volumes': cbol831bJob_vol,
        'volume_mounts': cbol831bJob_volMnt,
        'env_from':cbol831bJob_env,
        'task_id':'cbol831bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.1',
        'arguments':["--job.name=cbol831bJob",  "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('362a9b9718e34979933ca657df59cd7c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol831bJob,
        Complete
    ]) 

    # authCheck >> cbol831bJob >> Complete
    workflow








